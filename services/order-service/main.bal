import ballerina/http;
import ballerina/kafka;
import ballerina/log;
import ballerina/sql;

const SERVICE = "order-service";

type OrderItem record {| string menuItemId; string name; int quantity; decimal price; |};
type NewOrder record {| string customerId; string restaurantId; string deliveryAddress; OrderItem[] items; |};
type OrderRow record {|
    string id;
    @sql:Column {name: "customer_id"} string customerId;
    @sql:Column {name: "restaurant_id"} string restaurantId;
    @sql:Column {name: "delivery_address"} string deliveryAddress;
    string items;
    decimal total;
    string status;
    @sql:Column {name: "created_at"} string createdAt;
|};
type OrderView record {|
    string id;
    string customerId;
    string restaurantId;
    string deliveryAddress;
    json items;
    decimal total;
    string status;
    string createdAt;
|};

// Order lifecycle: CREATED -> CONFIRMED -> PREPARING -> READY -> OUT_FOR_DELIVERY -> DELIVERED (or CANCELLED)
final map<string[]> allowedFrom = {
    "CONFIRMED": ["CREATED"],
    "PREPARING": ["CONFIRMED"],
    "READY": ["PREPARING"],
    "OUT_FOR_DELIVERY": ["READY"],
    "DELIVERED": ["OUT_FOR_DELIVERY"],
    "CANCELLED": ["CREATED", "CONFIRMED"]
};

function init() returns error? {
    _ = check db->execute(`CREATE TABLE IF NOT EXISTS orders (
        id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, restaurant_id TEXT NOT NULL,
        delivery_address TEXT NOT NULL, items TEXT NOT NULL, total NUMERIC(12,2) NOT NULL,
        status TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)`);
}

const decimal BASE_DELIVERY_FEE = 15.0;

// Surge pricing: the delivery fee rises with demand (orders placed in the last 10 minutes).
function currentSurge() returns decimal|error {
    int recent = check db->queryRow(`SELECT COUNT(*) FROM orders WHERE created_at::timestamptz > NOW() - INTERVAL '10 minutes'`);
    if recent >= 6 {
        return 1.5d;
    }
    if recent >= 3 {
        return 1.25d;
    }
    return 1.0d;
}

function toView(OrderRow r) returns OrderView|error {
    json items = check value:fromJsonString(r.items);
    return {id: r.id, customerId: r.customerId, restaurantId: r.restaurantId,
        deliveryAddress: r.deliveryAddress, items, total: r.total, status: r.status, createdAt: r.createdAt};
}

function applyStatus(string orderId, string newStatus) returns error? {
    string current = check db->queryRow(`SELECT status FROM orders WHERE id = ${orderId}`);
    string[]? allowed = allowedFrom[newStatus];
    if allowed is string[] && allowed.indexOf(current) is int {
        _ = check db->execute(`UPDATE orders SET status = ${newStatus}, updated_at = ${now()} WHERE id = ${orderId}`);
        log:printInfo(string `Order ${orderId}: ${current} -> ${newStatus}`);
    } else {
        log:printWarn(string `Order ${orderId}: ignored transition ${current} -> ${newStatus}`);
    }
}

service /orders on new http:Listener(9093) {

    resource function post .(NewOrder req) returns http:Created|http:BadRequest|error {
        if req.items.length() == 0 {
            return <http:BadRequest>{body: "Order must contain at least one item"};
        }
        decimal subtotal = 0;
        foreach OrderItem i in req.items {
            subtotal += i.price * <decimal>i.quantity;
        }
        decimal surge = check currentSurge();
        decimal deliveryFee = BASE_DELIVERY_FEE * surge;
        decimal total = subtotal + deliveryFee;
        string id = newId();
        string ts = now();
        _ = check db->execute(`INSERT INTO orders (id, customer_id, restaurant_id, delivery_address, items, total, status, created_at, updated_at)
            VALUES (${id}, ${req.customerId}, ${req.restaurantId}, ${req.deliveryAddress}, ${req.items.toJsonString()},
            ${total}, 'CREATED', ${ts}, ${ts})`);
        check publish("order.created", {orderId: id, customerId: req.customerId, restaurantId: req.restaurantId,
            amount: total, deliveryAddress: req.deliveryAddress, status: "CREATED", timestamp: ts});
        return <http:Created>{body: {orderId: id, status: "CREATED", subtotal, deliveryFee, surgeMultiplier: surge, total}};
    }

    resource function get surge() returns json|error {
        decimal m = check currentSurge();
        return {multiplier: m, deliveryFee: BASE_DELIVERY_FEE * m};
    }

    resource function get [string id]() returns OrderView|http:NotFound|error {
        OrderRow|sql:Error r = db->queryRow(`SELECT id, customer_id, restaurant_id, delivery_address, items, total, status, created_at
            FROM orders WHERE id = ${id}`);
        if r is sql:NoRowsError {
            return http:NOT_FOUND;
        }
        if r is error {
            return r;
        }
        return toView(r);
    }

    resource function get .(string? customerId) returns OrderView[]|error {
        stream<OrderRow, sql:Error?> rs;
        if customerId is string {
            rs = db->query(`SELECT id, customer_id, restaurant_id, delivery_address, items, total, status, created_at
                FROM orders WHERE customer_id = ${customerId} ORDER BY created_at DESC`);
        } else {
            rs = db->query(`SELECT id, customer_id, restaurant_id, delivery_address, items, total, status, created_at
                FROM orders ORDER BY created_at DESC`);
        }
        OrderView[] out = [];
        check from OrderRow r in rs
            do {
                out.push(check toView(r));
            };
        return out;
    }

    resource function post [string id]/cancel() returns http:Ok|http:Conflict|http:NotFound|error {
        OrderRow|sql:Error r = db->queryRow(`SELECT id, customer_id, restaurant_id, delivery_address, items, total, status, created_at
            FROM orders WHERE id = ${id}`);
        if r is sql:NoRowsError {
            return http:NOT_FOUND;
        }
        if r is error {
            return r;
        }
        if r.status != "CREATED" && r.status != "CONFIRMED" {
            return <http:Conflict>{body: string `Cannot cancel an order that is ${r.status}`};
        }
        _ = check db->execute(`UPDATE orders SET status = 'CANCELLED', updated_at = ${now()} WHERE id = ${id}`);
        check publish("order.cancelled", {orderId: id, customerId: r.customerId, restaurantId: r.restaurantId,
            amount: r.total, deliveryAddress: r.deliveryAddress, status: "CANCELLED", reason: "Cancelled by customer",
            timestamp: now()});
        return <http:Ok>{body: {orderId: id, status: "CANCELLED"}};
    }
}

// Order service owns the state machine: it moves the order forward as other services publish events.
listener kafka:Listener statusListener = new (kafkaBootstrap, {
    groupId: "order-service-group",
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    topics: ["payment.failed", "order.confirmed", "order.preparing", "order.ready",
        "delivery.started", "delivery.completed"]
});

service kafka:Service on statusListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord r in records {
            error? res = handle(r.value);
            if res is error {
                log:printError("Order service failed to apply status", res);
            }
        }
    }
}

function handle(byte[] raw) returns error? {
    Event e = check parseEvent(raw);
    string? s = e?.status;
    if s is string {
        check applyStatus(e.orderId, s);
    }
}
