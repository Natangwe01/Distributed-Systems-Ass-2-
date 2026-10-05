import ballerina/kafka;

// Messages are keyed by orderId so every event of one order lands in the same partition (ordering per order).
final kafka:Producer producer = check new (kafkaBootstrap, {clientId: SERVICE, acks: "all", retryCount: 3});

function publish(string topic, Event e) returns error? {
    check producer->send({topic, key: e.orderId.toBytes(), value: e.toJsonString().toBytes()});
}
