import ballerina/http;
import ballerina/kafka;
import ballerina/log;
import ballerina/sql;

const SERVICE = "delivery-service";

type DeliveryRow record {|
    @sql:Column {name: "order_id"} string orderId;
    @sql:Column {name: "driver_id"} string? driverId;
    @sql:Column {name: "customer_id"} string customerId;
    @sql:Column {name: "restaurant_id"} string restaurantId;
    decimal amount;
    @sql:Column {name: "delivery_address"} string deliveryAddress;
    string status;
|};

type Location record {| float lat; float lon; |};
type LocationView record {|
    float? lat;
    float? lon;
    @sql:Column {name: "loc_updated_at"} string? updatedAt;
    string status;
|};

function init() returns error? {
    _ = check db->execute(`CREATE TABLE IF NOT EXISTS drivers (
        id TEXT PRIMARY KEY, name TEXT NOT NULL, available BOOLEAN NOT NULL DEFAULT TRUE)`);
    _ = check db->execute(`INSERT INTO drivers (id, name) VALUES ('driver-1', 'Aina'), ('driver-2', 'Tuli'), ('driver-3', 'Johannes')
        ON CONFLICT (id) DO NOTHING`);
    _ = check db->execute(`CREATE TABLE IF NOT EXISTS deliveries (
        order_id TEXT PRIMARY KEY, driver_id TEXT, customer_id TEXT NOT NULL, restaurant_id TEXT NOT NULL,
        amount NUMERIC(12,2) NOT NULL, delivery_address TEXT NOT NULL, status TEXT NOT NULL, updated_at TEXT NOT NULL)`);
    _ = check db->execute(`ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS lat DOUBLE PRECISION`);
    _ = check db->execute(`ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS lon DOUBLE PRECISION`);
    _ = check db->execute(`ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS loc_updated_at TEXT`);
}

function advance(string orderId, string expected, string target, string topic)
        returns http:Ok|http:NotFound|http:Conflict|error {
    DeliveryRow|sql:Error d = db->queryRow(`SELECT order_id, driver_id, customer_id, restaurant_id, amount, delivery_address, status
        FROM deliveries WHERE order_id = ${orderId}`);
    if d is sql:NoRowsError {
        return http:NOT_FOUND;
    }
    if d is error {
        return d;
    }
    if d.status != expected {
        return <http:Conflict>{body: string `Delivery is ${d.status}, expected ${expected}`};
    }
    _ = check db->execute(`UPDATE deliveries SET status = ${target}, updated_at = ${now()} WHERE order_id = ${orderId}`);
    string? driverId = d.driverId;
    if target == "DELIVERED" && driverId is string {
        _ = check db->execute(`UPDATE drivers SET available = TRUE WHERE id = ${driverId}`);
    }
    Event e = {orderId: d.orderId, customerId: d.customerId, restaurantId: d.restaurantId,
        amount: d.amount, deliveryAddress: d.deliveryAddress, timestamp: now()};
    check publish(topic, derive(e, target, driverId));
    return <http:Ok>{body: {orderId, status: target}};
}

service /deliveries on new http:Listener(9095) {

    resource function get [string orderId]() returns DeliveryRow|http:NotFound|error {
        DeliveryRow|sql:Error d = db->queryRow(`SELECT order_id, driver_id, customer_id, restaurant_id, amount, delivery_address, status
            FROM deliveries WHERE order_id = ${orderId}`);
        if d is sql:NoRowsError {
            return http:NOT_FOUND;
        }
        return d;
    }

    // Driver app (or the web UI's simulator) reports its position while delivering.
    resource function put [string orderId]/location(Location loc) returns http:Ok|http:Conflict|error {
        sql:ExecutionResult r = check db->execute(`UPDATE deliveries SET lat = ${loc.lat}, lon = ${loc.lon}, loc_updated_at = ${now()}
            WHERE order_id = ${orderId} AND status = 'OUT_FOR_DELIVERY'`);
        if r.affectedRowCount == 0 {
            return <http:Conflict>{body: "Delivery is not out for delivery"};
        }
        return <http:Ok>{body: {orderId, lat: loc.lat, lon: loc.lon}};
    }

    resource function get [string orderId]/location() returns LocationView|http:NotFound|error {
        LocationView|sql:Error v = db->queryRow(`SELECT lat, lon, loc_updated_at, status FROM deliveries WHERE order_id = ${orderId}`);
        if v is sql:NoRowsError {
            return http:NOT_FOUND;
        }
        return v;
    }

    // Driver app calls: picked up the food / handed it to the customer.
    resource function put [string orderId]/start() returns http:Ok|http:NotFound|http:Conflict|error {
        return advance(orderId, "ASSIGNED", "OUT_FOR_DELIVERY", "delivery.started");
    }

    resource function put [string orderId]/complete() returns http:Ok|http:NotFound|http:Conflict|error {
        return advance(orderId, "OUT_FOR_DELIVERY", "DELIVERED", "delivery.completed");
    }
}

listener kafka:Listener readyListener = new (kafkaBootstrap, {
    groupId: "delivery-service-group",
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    topics: ["order.ready"]
});

service kafka:Service on readyListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord r in records {
            error? res = assignDriver(r.value);
            if res is error {
                log:printError("Driver assignment failed", res);
            }
        }
    }
}

function assignDriver(byte[] raw) returns error? {
    Event e = check parseEvent(raw);
    // Atomically claim one free driver.
    string|sql:Error d = db->queryRow(`UPDATE drivers SET available = FALSE WHERE id =
        (SELECT id FROM drivers WHERE available = TRUE ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED) RETURNING id`);
    if d is sql:NoRowsError {
        _ = check db->execute(`INSERT INTO deliveries (order_id, driver_id, customer_id, restaurant_id, amount, delivery_address, status, updated_at)
            VALUES (${e.orderId}, NULL, ${e.customerId}, ${e.restaurantId}, ${e.amount}, ${e.deliveryAddress}, 'PENDING', ${now()})
            ON CONFLICT (order_id) DO NOTHING`);
        log:printWarn(string `No driver available for order ${e.orderId}; delivery is PENDING`);
        return;
    }
    if d is error {
        return d;
    }
    _ = check db->execute(`INSERT INTO deliveries (order_id, driver_id, customer_id, restaurant_id, amount, delivery_address, status, updated_at)
        VALUES (${e.orderId}, ${d}, ${e.customerId}, ${e.restaurantId}, ${e.amount}, ${e.deliveryAddress}, 'ASSIGNED', ${now()})
        ON CONFLICT (order_id) DO NOTHING`);
    check publish("delivery.assigned", derive(e, driverId = d));
}
