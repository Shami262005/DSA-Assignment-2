import ballerina/log;
import ballerina/sql;
import ballerinax/kafka;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;

configurable string kafkaEndpoint = "localhost:9092, localhost:9093 ";

public type Deliveries record {

    int deliveryId?;
    string customerName?;
    string contactNumber?;
    string pickUpLocation?;
    string deliveryLocation?;
    string preferred_time?;
    string tracking_id?;
    string deliverytype?;

};

public type DeliverySchedule record {
    int scheduleId?;
    string deliveryName?;
    string deliveryDay?;
};

// change password to ur mysql password, and the database name to what u named ur database
final mysql:Client dbClient = check new (host = "localhost", user = "root", password = "Breezy@04", port = 3306,
    database = "logisticsystem", connectionPool = {maxOpenConnections: 3, minIdleConnections: 1}
);

//configuration of the consumer
final kafka:ConsumerConfiguration consumerConfiguration = {
        
        groupId: "ConsumerRecieveRequest",
        offsetReset: "earliest",
        topics: ["new_delivery_requests"]
        };

//producer intailization
service on new kafka:Listener(kafkaEndpoint, consumerConfiguration) {

    private final kafka:Producer orderProducer;

    function init() returns error? {

        kafka:ProducerConfiguration producerConfiguration = {
            clientId: "ProducerProcessesRequest",
            acks: "all",
            retryCount: 3
};

        self.orderProducer = check new (kafkaEndpoint, producerConfiguration);

    }

    remote function onConsumerRecord(kafka:Caller simpleConsumer, kafka:BytesConsumerRecord[] records) returns error? {

        foreach kafka:BytesConsumerRecord entry in records {

            string message = check string:fromBytes(entry.value);
            json deliveryJson = check message.fromJsonString();
            Deliveries delivery = check deliveryJson.cloneWithType(Deliveries);

            check addInfo(delivery);

            string topicTosend;
            if delivery.deliverytype == "standard" {
                topicTosend = "standard_delivery_requests";
            } else if delivery.deliverytype == "international" {
                topicTosend = "international_delivery_requests";
            } else if delivery.deliverytype == "express" {
                topicTosend = "express_delivery_requests";
            } else {
                log:printError("Unknown delivery Type");
                continue;
            }

            check self.orderProducer->send({
                topic: topicTosend,
                value: delivery.toString()
            });

        }
    }

}

function addInfo(Deliveries SD) returns error? {

    sql:ExecutionResult _ = check dbClient->execute(`INSERT INTO deliveries (delivery_id, customer_name, contact_number, pickup_location, delivery_location, delivery_type, preferred_times, tracking_id) 
        VALUES (${SD.deliveryId},${SD.customerName},${SD.contactNumber}, ${SD.pickUpLocation}, ${SD.deliveryLocation}, ${SD.deliverytype}, ${SD.preferred_time}, ${SD.tracking_id})`);

return ;

}
