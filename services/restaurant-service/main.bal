import ballerina/http;
import ballerina/kafka;
import ballerina/log;
import ballerina/sql;

const SERVICE = "restaurant-service";

type NewRestaurant record {| string name; string address; |};
type Restaurant record {| string id; string name; string address; |};
type NewMenuItem record {| string name; decimal price; |};
type MenuItem record {| string id; string name; decimal price; |};
type RestOrder record {|
    @sql:Column {name: "order_id"} string orderId;
    @sql:Column {name: "customer_id"} string customerId;
    @sql:Column {name: "restaurant_id"} string restaurantId;
    @sql:Column {name: "delivery_address"} string deliveryAddress;
    decimal amount;
    string status;
|};

function init() returns error? {
    _ = check db->execute(`CREATE TABLE IF NOT EXISTS restaurants (
        id TEXT PRIMARY KEY, name TEXT NOT NULL, address TEXT NOT NULL)`);
    _ = check db->execute(`CREATE TABLE IF NOT EXISTS menu_items (
        id TEXT PRIMARY KEY, restaurant_id TEXT NOT NULL REFERENCES restaurants(id),
        name TEXT NOT NULL, price NUMERIC(10,2) NOT NULL)`);
    _ = check db->execute(`CREATE TABLE IF NOT EXISTS restaurant_orders (
        order_id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, restaurant_id TEXT NOT NULL,
        delivery_address TEXT NOT NULL, amount NUMERIC(12,2) NOT NULL, status TEXT NOT NULL)`);
}

function toEvent(RestOrder o) returns Event {
    return {orderId: o.orderId, customerId: o.customerId, restaurantId: o.restaurantId,
        amount: o.amount, deliveryAddress: o.deliveryAddress, timestamp: now()};
}

function advance(string orderId, string expected, string target, string topic)
        returns http:Ok|http:NotFound|http:Conflict|error {
    RestOrder|sql:Error o = db->queryRow(`SELECT order_id, customer_id, restaurant_id, delivery_address, amount, status
        FROM restaurant_orders WHERE order_id = ${orderId}`);
    if o is sql:NoRowsError {
        return http:NOT_FOUND;
    }
    if o is error {
        return o;
    }
    if o.status != expected {
        return <http:Conflict>{body: string `Order is ${o.status}, expected ${expected}`};
    }
    _ = check db->execute(`UPDATE restaurant_orders SET status = ${target} WHERE order_id = ${orderId}`);
    check publish(topic, derive(toEvent(o), target));
    return <http:Ok>{body: {orderId, status: target}};
}

service /restaurants on new http:Listener(9092) {

    resource function post .(NewRestaurant r) returns http:Created|error {
        string id = newId();
        _ = check db->execute(`INSERT INTO restaurants (id, name, address) VALUES (${id}, ${r.name}, ${r.address})`);
        return <http:Created>{body: <Restaurant>{id, name: r.name, address: r.address}};
    }

    resource function get .() returns Restaurant[]|error {
        stream<Restaurant, sql:Error?> rs = db->query(`SELECT id, name, address FROM restaurants`);
        Restaurant[] out = [];
        check from Restaurant r in rs
            do {
                out.push(r);
            };
        return out;
    }

    resource function post [string id]/menu(NewMenuItem m) returns http:Created|http:NotFound|error {
        int|sql:Error exists = db->queryRow(`SELECT COUNT(*) FROM restaurants WHERE id = ${id}`);
        if exists is error || exists == 0 {
            return http:NOT_FOUND;
        }
        string mid = newId();
        _ = check db->execute(`INSERT INTO menu_items (id, restaurant_id, name, price)
            VALUES (${mid}, ${id}, ${m.name}, ${m.price})`);
        return <http:Created>{body: <MenuItem>{id: mid, name: m.name, price: m.price}};
    }

    resource function get [string id]/menu() returns MenuItem[]|error {
        stream<MenuItem, sql:Error?> rs = db->query(`SELECT id, name, price FROM menu_items WHERE restaurant_id = ${id}`);
        MenuItem[] out = [];
        check from MenuItem m in rs
            do {
                out.push(m);
            };
        return out;
    }

    resource function get [string id]/orders() returns RestOrder[]|error {
        stream<RestOrder, sql:Error?> rs = db->query(`SELECT order_id, customer_id, restaurant_id, delivery_address, amount, status
            FROM restaurant_orders WHERE restaurant_id = ${id}`);
        RestOrder[] out = [];
        check from RestOrder o in rs
            do {
                out.push(o);
            };
        return out;
    }

    resource function put orders/[string orderId]/preparing() returns http:Ok|http:NotFound|http:Conflict|error {
        return advance(orderId, "CONFIRMED", "PREPARING", "order.preparing");
    }

    resource function put orders/[string orderId]/ready() returns http:Ok|http:NotFound|http:Conflict|error {
        return advance(orderId, "PREPARING", "READY", "order.ready");
    }
}

listener kafka:Listener paymentListener = new (kafkaBootstrap, {
    groupId: "restaurant-service-group",
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    topics: ["payment.completed", "order.cancelled"]
});

service kafka:Service on paymentListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord r in records {
            error? res = handle(r.offset.partition.topic, r.value);
            if res is error {
                log:printError("Restaurant failed to handle event", res);
            }
        }
    }
}

function handle(string topic, byte[] raw) returns error? {
    Event e = check parseEvent(raw);
    if topic == "payment.completed" {
        // Orders are auto-accepted once payment has cleared.
        _ = check db->execute(`INSERT INTO restaurant_orders (order_id, customer_id, restaurant_id, delivery_address, amount, status)
            VALUES (${e.orderId}, ${e.customerId}, ${e.restaurantId}, ${e.deliveryAddress}, ${e.amount}, 'CONFIRMED')
            ON CONFLICT (order_id) DO NOTHING`);
        check publish("order.confirmed", derive(e, "CONFIRMED"));
    } else if topic == "order.cancelled" {
        _ = check db->execute(`UPDATE restaurant_orders SET status = 'CANCELLED' WHERE order_id = ${e.orderId}`);
    }
}
