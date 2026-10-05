import ballerina/http;
import ballerina/kafka;
import ballerina/log;
import ballerina/sql;

const SERVICE = "customer-service";

type NewCustomer record {| string name; string email; string phone; |};
type Customer record {| string id; string name; string email; string phone; |};
type NewAddress record {| string label; string address; |};
type Address record {| string id; string label; string address; |};
type HistoryRow record {|
    @sql:Column {name: "order_id"} string orderId;
    @sql:Column {name: "restaurant_id"} string restaurantId;
    decimal amount;
    string status;
    @sql:Column {name: "updated_at"} string updatedAt;
|};

function init() returns error? {
    _ = check db->execute(`CREATE TABLE IF NOT EXISTS customers (
        id TEXT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE NOT NULL,
        phone TEXT NOT NULL, created_at TEXT NOT NULL)`);
    _ = check db->execute(`CREATE TABLE IF NOT EXISTS addresses (
        id TEXT PRIMARY KEY, customer_id TEXT NOT NULL REFERENCES customers(id),
        label TEXT NOT NULL, address TEXT NOT NULL)`);
    _ = check db->execute(`CREATE TABLE IF NOT EXISTS order_history (
        order_id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, restaurant_id TEXT NOT NULL,
        amount NUMERIC(12,2) NOT NULL, status TEXT NOT NULL, updated_at TEXT NOT NULL)`);
}

service /customers on new http:Listener(9091) {

    resource function post .(NewCustomer c) returns http:Created|http:Conflict|error {
        string id = newId();
        sql:ExecutionResult|sql:Error r = db->execute(`INSERT INTO customers (id, name, email, phone, created_at)
            VALUES (${id}, ${c.name}, ${c.email}, ${c.phone}, ${now()})`);
        if r is sql:DatabaseError {
            return <http:Conflict>{body: "Email already registered"};
        }
        if r is error {
            return r;
        }
        return <http:Created>{body: <Customer>{id, name: c.name, email: c.email, phone: c.phone}};
    }

    resource function get [string id]() returns Customer|http:NotFound|error {
        Customer|sql:Error c = db->queryRow(`SELECT id, name, email, phone FROM customers WHERE id = ${id}`);
        if c is sql:NoRowsError {
            return http:NOT_FOUND;
        }
        return c;
    }

    resource function post [string id]/addresses(NewAddress a) returns http:Created|http:NotFound|error {
        int|sql:Error exists = db->queryRow(`SELECT COUNT(*) FROM customers WHERE id = ${id}`);
        if exists is error || exists == 0 {
            return http:NOT_FOUND;
        }
        string aid = newId();
        _ = check db->execute(`INSERT INTO addresses (id, customer_id, label, address)
            VALUES (${aid}, ${id}, ${a.label}, ${a.address})`);
        return <http:Created>{body: <Address>{id: aid, label: a.label, address: a.address}};
    }

    resource function get [string id]/addresses() returns Address[]|error {
        stream<Address, sql:Error?> rs = db->query(`SELECT id, label, address FROM addresses WHERE customer_id = ${id}`);
        Address[] out = [];
        check from Address a in rs
            do {
                out.push(a);
            };
        return out;
    }

    // Order history, built from Kafka events (no call to the Order service needed).
    resource function get [string id]/orders() returns HistoryRow[]|error {
        stream<HistoryRow, sql:Error?> rs = db->query(`SELECT order_id, restaurant_id, amount, status, updated_at
            FROM order_history WHERE customer_id = ${id} ORDER BY updated_at DESC`);
        HistoryRow[] out = [];
        check from HistoryRow h in rs
            do {
                out.push(h);
            };
        return out;
    }
}

listener kafka:Listener historyListener = new (kafkaBootstrap, {
    groupId: "customer-service-group",
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    topics: ["order.created", "payment.failed", "order.confirmed", "order.preparing", "order.ready",
        "order.cancelled", "delivery.started", "delivery.completed"]
});

service kafka:Service on historyListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord r in records {
            error? res = recordHistory(r.value);
            if res is error {
                log:printError("Failed to record order history", res);
            }
        }
    }
}

function recordHistory(byte[] raw) returns error? {
    Event e = check parseEvent(raw);
    string? status = e?.status;
    if status is string {
        _ = check db->execute(`INSERT INTO order_history (order_id, customer_id, restaurant_id, amount, status, updated_at)
            VALUES (${e.orderId}, ${e.customerId}, ${e.restaurantId}, ${e.amount}, ${status}, ${e.timestamp})
            ON CONFLICT (order_id) DO UPDATE SET status = EXCLUDED.status, updated_at = EXCLUDED.updated_at`);
    }
}
