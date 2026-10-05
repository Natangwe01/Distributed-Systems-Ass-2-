import ballerina/http;
import ballerina/kafka;
import ballerina/log;
import ballerina/sql;

type NotificationRow record {|
    int id;
    @sql:Column {name: "order_id"} string orderId;
    string recipient;
    string message;
    @sql:Column {name: "created_at"} string createdAt;
|};

function init() returns error? {
    _ = check db->execute(`CREATE TABLE IF NOT EXISTS notifications (
        id SERIAL PRIMARY KEY, order_id TEXT NOT NULL, recipient TEXT NOT NULL,
        message TEXT NOT NULL, created_at TEXT NOT NULL)`);
}

function messagesFor(string topic, Event e) returns [string, string][] {
    string o = e.orderId;
    match topic {
        "order.created" => {
            return [["customer", string `Order ${o} placed. Waiting for payment.`]];
        }
        "payment.completed" => {
            return [["customer", string `Payment received for order ${o}.`]];
        }
        "payment.failed" => {
            return [["customer", string `Payment failed for order ${o}: ${e?.reason ?: "unknown reason"}.`]];
        }
        "order.confirmed" => {
            return [["customer", string `Restaurant confirmed order ${o}.`],
                ["restaurant", string `New order ${o} to prepare.`]];
        }
        "order.preparing" => {
            return [["customer", string `Your order ${o} is being prepared.`]];
        }
        "order.ready" => {
            return [["customer", string `Order ${o} is ready and waiting for a driver.`]];
        }
        "delivery.assigned" => {
            return [["customer", string `Driver ${e?.driverId ?: "?"} assigned to order ${o}.`],
                ["driver", string `Pick up order ${o} and deliver to ${e.deliveryAddress}.`]];
        }
        "delivery.started" => {
            return [["customer", string `Order ${o} is on its way.`]];
        }
        "delivery.completed" => {
            return [["customer", string `Order ${o} delivered. Enjoy!`],
                ["restaurant", string `Order ${o} was delivered.`],
                ["driver", string `Delivery of order ${o} recorded.`]];
        }
        "order.cancelled" => {
            return [["customer", string `Order ${o} was cancelled.`],
                ["restaurant", string `Order ${o} was cancelled.`]];
        }
    }
    return [];
}

listener kafka:Listener allEvents = new (kafkaBootstrap, {
    groupId: "notification-service-group",
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    topics: ["order.created", "payment.completed", "payment.failed", "order.confirmed", "order.preparing",
        "order.ready", "order.cancelled", "delivery.assigned", "delivery.started", "delivery.completed"]
});

service kafka:Service on allEvents {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord r in records {
            error? res = notify(r.offset.partition.topic, r.value);
            if res is error {
                log:printError("Notification failed", res);
            }
        }
    }
}

function notify(string topic, byte[] raw) returns error? {
    Event e = check parseEvent(raw);
    foreach [string, string] [recipient, message] in messagesFor(topic, e) {
        _ = check db->execute(`INSERT INTO notifications (order_id, recipient, message, created_at)
            VALUES (${e.orderId}, ${recipient}, ${message}, ${now()})`);
        // A real system would call email/SMS/push providers here.
        log:printInfo(string `[${recipient}] ${message}`);
    }
}

service /notifications on new http:Listener(9096) {
    resource function get .(string? orderId) returns NotificationRow[]|error {
        stream<NotificationRow, sql:Error?> rs;
        if orderId is string {
            rs = db->query(`SELECT id, order_id, recipient, message, created_at FROM notifications
                WHERE order_id = ${orderId} ORDER BY id`);
        } else {
            rs = db->query(`SELECT id, order_id, recipient, message, created_at FROM notifications ORDER BY id DESC LIMIT 100`);
        }
        NotificationRow[] out = [];
        check from NotificationRow n in rs
            do {
                out.push(n);
            };
        return out;
    }
}
