import ballerina/io;
import ballerinax/kafka;

//producer

configurable string kafkaEndpointRq = "localhost:9092, localhost:9093 ";

kafka:ProducerConfiguration producerConfiguration = {
    clientId: "Customer1",
    acks: "all",
    retryCount: 3
};

kafka:Producer kafkaProducer = check new (kafkaEndpointRq, producerConfiguration);

function sendMessage(string sendMessage) returns error? {

    string message = sendMessage;

    check kafkaProducer->send({topic: "new_dlivery_requests", value: message});

}


//Consumer

configurable string kafkaEndpointRcv = "localhost:9092, localhost:9094";


kafka:ConsumerConfiguration consumerConfiguration = {
    groupId: "Taapopi",
    offsetReset: "earliest",
    topics: ["delivery_schedule_response"]

};

kafka:Consumer kafkaConsumer = check new (kafkaEndpointRcv, consumerConfiguration);
listener kafka:Listener kafkaListener = new (kafkaEndpointRcv, consumerConfiguration);

service on kafkaListener {

    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {

        foreach var kafkaRecord in records {
            byte[] messageContent = kafkaRecord.value;
            string result = check string:fromBytes(messageContent);
            io:println("The result is : ", result);
        }

    }
}



public function main() returns error? {

    io:println("Enter delivery_id: ");
    string deliveryId = io:readln();

    io:println("Enter customer_name: ");
    string customerName = io:readln();

    io:println("Enter contact_number: ");
    string contactNumber = io:readln();

    io:println("Enter pickup_location: ");
    string pickupLocation = io:readln();

    io:println("Enter delivery_location: ");
    string deliveryLocation = io:readln();

    io:println("Enter delivery_type (standard/express/international): ");
    string deliveryType = io:readln();

    io:println("Enter preferred_times: ");
    string preferredTimes = io:readln();

    io:println("Enter tracking_id: ");
    string trackingId = io:readln();

    string deliveryMessage = "{ \"delivery_id\": \"" + deliveryId + "\", \"customer_name\": \"" + customerName +
        "\", \"contact_number\": \"" + contactNumber + "\", \"pickup_location\": \"" + pickupLocation +
        "\", \"delivery_location\": \"" + deliveryLocation + "\", \"delivery_type\": \"" + deliveryType +
        "\", \"preferred_times\": \"" + preferredTimes + "\", \"tracking_id\": \"" + trackingId + "\" }";

    check sendMessage(deliveryMessage);

    io:print("Delivery Details sent successfullt, Waiting for response.....: ");

}
   