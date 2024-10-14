import ballerina/io;
import ballerina/log;
import ballerina/sql;
import ballerinax/kafka;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;

type CustomerRequest readonly & record {
    string id;
    string firstName;
    string lastName;
    string contactNumber;
    string shipmentType;
    string pickupLocation;
    string deliveryLocation;
    string preferredTimeSlot;
};

type CustomerResponse readonly & record {

};

type responses record {
    string tracking_id;
    string customer_id;
    string firstName;
    string lastName;
    string contactNumber;
    string shipmentType;
    string pickupLocation;
    string deliveryLocation;
    string pickup_time;
    string estimated_delivery_time;
};

// Kafka configuration
listener kafka:Listener kafkaListener = new (kafka:DEFAULT_URL, {
    groupId: "customer_request_group",
    topics: ["new-delivery-requests"]
});

final kafka:Producer standardProducer = check new (kafka:DEFAULT_URL);
final kafka:Producer expressProducer = check new (kafka:DEFAULT_URL);
final kafka:Producer internationalProducer = check new (kafka:DEFAULT_URL);
final kafka:Producer logisticsResponseProducer = check new (kafka:DEFAULT_URL);

final kafka:Consumer standardResponseConsumer = check new (kafka:DEFAULT_URL, {groupId: "standard_group", topics: ["standard-delivery-response"]});
final kafka:Consumer expressResponseConsumer = check new (kafka:DEFAULT_URL, {groupId: "express_group", topics: ["express-delivery-response"]});
final kafka:Consumer internationalResponseConsumer = check new (kafka:DEFAULT_URL, {groupId: "international_group", topics: ["international-delivery-response"]});

// Database connection
mysql:Client dbClient = check new (user = "root", password = "Victoria@1509", database = "logistics_service", host = "localhost", port = 3306);

// Main service for incoming requests
service on kafkaListener {
    remote function onConsumerRecord(CustomerRequest[] customerRequests) returns kafka:Error? {
        foreach var request in customerRequests {
            io:println(string `Received customer request from ${request.firstName} ${request.lastName}`);
            var dbResult = storeCustomerRequest(request);
            if (dbResult is sql:ExecutionResult) {
                log:printInfo("Successfully inserted customer request into the database.");

                json payload = {
                    customerID: request.id,
                    firstName: request.firstName,
                    lastName: request.lastName,
                    contactNumber: request.contactNumber,
                    pickupLocation: request.pickupLocation,
                    shipmentType: request.shipmentType,
                    deliveryLocation: request.deliveryLocation,
                    preferredTimeSlot: request.preferredTimeSlot
                };

                // Handle the shipment based on type asynchronously
                if (request.shipmentType == "standard") {
                    _ = start handleStandarddeliveries(payload, request.id);
                } else if (request.shipmentType == "express") {
                    _ = start handleExpressdeliveries(payload, request.id);
                } else if (request.shipmentType == "international") {
                    _ = start handleInternationaldeliveries(payload, request.id);
                }

                io:println("Sent request to appropriate Kafka topic.");
            } else {
                io:println("Failed to insert customer request into the database: ", dbResult.message());
            }
        }
    }
}

// Function to handle standard deliveries
isolated function handleStandarddeliveries(json payload, string customer_id) returns kafka:Error|error? {
    check standardProducer->send({topic: "standard-delivery-requests", value: payload.toString()});
    responses response = check HandleStandardResponses(customer_id);

    io:println("Sending standard delivery response for request ID: ", response.customer_id);
    check logisticsResponseProducer->send({topic: "Logistics-service-response", value: response.toString()});
}

// Function to handle express deliveries
isolated function handleExpressdeliveries(json payload, string customer_id) returns kafka:Error?|error {
    check expressProducer->send({topic: "express-delivery-requests", value: payload.toString()});
    responses response = check HandleExpressResponses(customer_id);

    io:println("Sending standard delivery response for request ID: ", response.customer_id);
    check logisticsResponseProducer->send({topic: "Logistics-service-response", value: response.toString()});
}

// Function to handle international deliveries
isolated function handleInternationaldeliveries(json payload, string customer_id) returns kafka:Error?|error {
    check internationalProducer->send({topic: "international-delivery-requests", value: payload.toString()});
    responses response = check HandleInternationalResponses(customer_id);

    io:println("Sending standard delivery response for request ID: ", response.customer_id);
    check logisticsResponseProducer->send({topic: "Logistics-service-response", value: response.toString()});
}

// Listen for responses on standard delivery response topic

isolated function HandleStandardResponses(string customer_ID) returns responses|error {

    responses[] standardresponses = check standardResponseConsumer->pollPayload(1000);

    foreach var response in standardresponses {
        if response.customer_id.toString() == customer_ID.toString() {
            return response;

        }
    }
    return error("No matching response found for requestID: " + customer_ID);
}

isolated function HandleExpressResponses(string customer_ID) returns responses|error {
    responses[] expressresponses = check expressResponseConsumer->pollPayload(1000);

    foreach var response in expressresponses {
        if response.customer_id.toString() == customer_ID.toString() {
            return response;
        }
    }
    return error("No matching response found for requestID: " + customer_ID);
}

// Listen for responses on international delivery response topic
isolated function HandleInternationalResponses(string customer_ID) returns responses|error {
    responses[] internationalresponses = check internationalResponseConsumer->pollPayload(1000);

    foreach var response in internationalresponses {
        if response.customer_id.toString() == customer_ID.toString() {
            return response;
        }
    }
    return error("No matching response found for requestID: " + customer_ID);
}

// Function to store customer request in the database
function storeCustomerRequest(CustomerRequest request) returns sql:ExecutionResult|sql:Error {
    sql:ParameterizedQuery query = `INSERT INTO customer_requests
                                    (id,first_name, last_name, contact_number, shipment_type, pickup_location, 
                                     delivery_location, preferred_time_slot) 
                                     VALUES (${request.id},${request.firstName}, ${request.lastName}, ${request.contactNumber}, 
                                     ${request.shipmentType}, ${request.pickupLocation}, ${request.deliveryLocation}, 
                                     ${request.preferredTimeSlot})`;

    io:println("Executing query: ", query);
    return dbClient->execute(query);
}
