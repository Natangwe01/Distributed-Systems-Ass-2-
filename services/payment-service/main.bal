import ballerina/http;
import ballerina/kafka;
import ballerina/log;
import ballerina/sql;

const SERVICE = "payment-service";

type PaymentRow record {|
    string id;
    @sql:Column {name: "order_id"} string orderId;
    decimal amount;
    string status;
    @sql:Column {name: "created_at"} string createdAt;
|};

function init() returns error? {
    _ = check db->execute(`CREATE TABLE IF NOT EXISTS payments (
        id TEXT PRIMARY KEY, order_id TEXT UNIQUE NOT NULL, amount NUMERIC(12,2) NOT NULL,
        status TEXT NOT NULL, created_at TEXT NOT NULL)`);
}

service /payments on new http:Listener(9094) {
    resource function get [string orderId]() returns PaymentRow|http:NotFound|error {
        PaymentRow|sql:Error p = db->queryRow(`SELECT id, order_id, amount, status, created_at FROM payments WHERE order_id = ${orderId}`);
        if p is sql:NoRowsError {
            return http:NOT_FOUND;
        }
        return p;
    }
}

listener kafka:Listener orderListener = new (kafkaBootstrap, {
    groupId: "payment-service-group",
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    topics: ["order.created"]
});

service kafka:Service on orderListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord r in records {
            error? res = process(r.value);
            if res is error {
                log:printError("Payment processing failed", res);
            }
        }
    }
}

// Simulated gateway: payments above 5000 are declined.
function process(byte[] raw) returns error? {
    Event e = check parseEvent(raw);
    boolean approved = e.amount > 0d && e.amount <= 5000d;
    string status = approved ? "COMPLETED" : "FAILED";
    sql:ExecutionResult res = check db->execute(`INSERT INTO payments (id, order_id, amount, status, created_at)
        VALUES (${newId()}, ${e.orderId}, ${e.amount}, ${status}, ${now()}) ON CONFLICT (order_id) DO NOTHING`);
    if res.affectedRowCount == 0 {
        return; // already processed (redelivered message)
    }
    if approved {
        check publish("payment.completed", derive(e));
    } else {
        check publish("payment.failed", derive(e, "CANCELLED", reason = "Payment declined"));
    }
}
