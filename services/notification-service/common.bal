import ballerina/os;
import ballerina/time;
import ballerina/uuid;
import ballerinax/postgresql;
import ballerinax/postgresql.driver as _;
import ballerinax/prometheus as _;

function env(string key, string fallback) returns string {
    string v = os:getEnv(key);
    return v == "" ? fallback : v;
}

final string kafkaBootstrap = env("KAFKA_BOOTSTRAP", "localhost:9092");

final postgresql:Client db = check new (env("DB_HOST", "localhost"), env("DB_USER", "postgres"),
    env("DB_PASS", "postgres"), env("DB_NAME", "app"), check int:fromString(env("DB_PORT", "5432")));

// Event envelope shared by every topic. `status` is the order status the event implies (if any).
type Event record {
    string orderId;
    string customerId;
    string restaurantId;
    decimal amount;
    string deliveryAddress;
    string status?;
    string driverId?;
    string reason?;
    string timestamp;
};

function now() returns string {
    return time:utcToString(time:utcNow());
}

function newId() returns string {
    return uuid:createType1AsString();
}

function parseEvent(byte[] raw) returns Event|error {
    string s = check string:fromBytes(raw);
    json j = check value:fromJsonString(s);
    Event e = check j.cloneWithType();
    return e;
}

// Builds a follow-up event from an incoming one.
function derive(Event e, string? status = (), string? driverId = (), string? reason = ()) returns Event {
    Event n = {
        orderId: e.orderId,
        customerId: e.customerId,
        restaurantId: e.restaurantId,
        amount: e.amount,
        deliveryAddress: e.deliveryAddress,
        timestamp: now()
    };
    if status is string {
        n.status = status;
    }
    string? d = driverId ?: e?.driverId;
    if d is string {
        n.driverId = d;
    }
    if reason is string {
        n.reason = reason;
    }
    return n;
}
