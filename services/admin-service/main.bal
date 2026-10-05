import ballerina/http;
import ballerina/kafka;
import ballerina/log;
import ballerina/sql;

type RestaurantStats record {|
    @sql:Column {name: "restaurant_id"} string restaurantId;
    @sql:Column {name: "orders_total"} int ordersTotal;
    @sql:Column {name: "orders_delivered"} int ordersDelivered;
    @sql:Column {name: "orders_cancelled"} int ordersCancelled;
    decimal revenue;
|};
type DriverStats record {|
    @sql:Column {name: "driver_id"} string driverId;
    int deliveries;
|};
type Summary record {|
    @sql:Column {name: "orders_total"} int ordersTotal;
    @sql:Column {name: "orders_delivered"} int ordersDelivered;
    @sql:Column {name: "orders_cancelled"} int ordersCancelled;
    decimal revenue;
|};

function init() returns error? {
    _ = check db->execute(`CREATE TABLE IF NOT EXISTS restaurant_stats (
        restaurant_id TEXT PRIMARY KEY, orders_total INT NOT NULL DEFAULT 0, orders_delivered INT NOT NULL DEFAULT 0,
        orders_cancelled INT NOT NULL DEFAULT 0, revenue NUMERIC(14,2) NOT NULL DEFAULT 0)`);
    _ = check db->execute(`CREATE TABLE IF NOT EXISTS driver_stats (
        driver_id TEXT PRIMARY KEY, deliveries INT NOT NULL DEFAULT 0)`);
}

listener kafka:Listener statsListener = new (kafkaBootstrap, {
    groupId: "admin-service-group",
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    topics: ["order.created", "payment.failed", "order.cancelled", "delivery.completed"]
});

service kafka:Service on statsListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord r in records {
            error? res = update(r.offset.partition.topic, r.value);
            if res is error {
                log:printError("Stats update failed", res);
            }
        }
    }
}

function update(string topic, byte[] raw) returns error? {
    Event e = check parseEvent(raw);
    string rid = e.restaurantId;
    match topic {
        "order.created" => {
            _ = check db->execute(`INSERT INTO restaurant_stats (restaurant_id, orders_total) VALUES (${rid}, 1)
                ON CONFLICT (restaurant_id) DO UPDATE SET orders_total = restaurant_stats.orders_total + 1`);
        }
        "payment.failed"|"order.cancelled" => {
            _ = check db->execute(`INSERT INTO restaurant_stats (restaurant_id, orders_cancelled) VALUES (${rid}, 1)
                ON CONFLICT (restaurant_id) DO UPDATE SET orders_cancelled = restaurant_stats.orders_cancelled + 1`);
        }
        "delivery.completed" => {
            _ = check db->execute(`INSERT INTO restaurant_stats (restaurant_id, orders_delivered, revenue) VALUES (${rid}, 1, ${e.amount})
                ON CONFLICT (restaurant_id) DO UPDATE SET orders_delivered = restaurant_stats.orders_delivered + 1,
                revenue = restaurant_stats.revenue + ${e.amount}`);
            string? driver = e?.driverId;
            if driver is string {
                _ = check db->execute(`INSERT INTO driver_stats (driver_id, deliveries) VALUES (${driver}, 1)
                    ON CONFLICT (driver_id) DO UPDATE SET deliveries = driver_stats.deliveries + 1`);
            }
        }
    }
}

service /admin/stats on new http:Listener(9097) {

    resource function get restaurants() returns RestaurantStats[]|error {
        stream<RestaurantStats, sql:Error?> rs = db->query(`SELECT restaurant_id, orders_total, orders_delivered,
            orders_cancelled, revenue FROM restaurant_stats ORDER BY revenue DESC`);
        RestaurantStats[] out = [];
        check from RestaurantStats s in rs
            do {
                out.push(s);
            };
        return out;
    }

    resource function get drivers() returns DriverStats[]|error {
        stream<DriverStats, sql:Error?> rs = db->query(`SELECT driver_id, deliveries FROM driver_stats ORDER BY deliveries DESC`);
        DriverStats[] out = [];
        check from DriverStats s in rs
            do {
                out.push(s);
            };
        return out;
    }

    resource function get summary() returns Summary|error {
        return db->queryRow(`SELECT COALESCE(SUM(orders_total),0)::bigint AS orders_total,
            COALESCE(SUM(orders_delivered),0)::bigint AS orders_delivered,
            COALESCE(SUM(orders_cancelled),0)::bigint AS orders_cancelled,
            COALESCE(SUM(revenue),0)::numeric AS revenue FROM restaurant_stats`);
    }
}
