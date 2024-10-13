import ballerina/io;
import ballerina/random;
import ballerina/sql;
import ballerinax/kafka;
import ballerinax/mysql;

// Define the international delivery request record
type InternationalDeliveryRequest record {
    string id;
    string firstName;
    string lastName;
    string contactNumber;
    string pickupLocation;
    string shipmentType;
    string deliveryLocation;
    string preferredTimeSlot;
    string country;
};

type Timeslot record {
    int id;
    int year;
    int month;
    int day;
    string time;
};

// Kafka listener for receiving international delivery requests
listener kafka:Listener kafkaListener = new (kafka:DEFAULT_URL, {
    groupId: "international_delivery_group",
    topics: ["international-delivery-request"]
});

// Kafka producer to send responses back
kafka:Producer kafkaProducer = check new (kafka:DEFAULT_URL);

// MySQL client setup
mysql:Client dbClient = check new (user = "root", password = "Victoria@1509", database = "Assignment2", host = "localhost", port = 3306);

// Kafka service that listens for incoming international delivery requests
service on kafkaListener {

    remote function onConsumerRecord(InternationalDeliveryRequest[] requests) returns kafka:Error? {
        foreach var request in requests {
            io:println(string `Received international delivery request from ${request.firstName} ${request.lastName}`);
            var Result = processInternationalDeliveryRequest(request);
            if (Result is error) {
                io:println("Error processing request: ", Result.message());
            } else {
                io:println("Successfully processed the international delivery request.");
            }
        }
    }
}

// Function to process each international delivery request
function processInternationalDeliveryRequest(InternationalDeliveryRequest request) returns error? {
    io:println("Processing international delivery request for: ", request.firstName, " ", request.lastName);
    io:println("Preferred time slot: ", request.preferredTimeSlot);

    Timeslot|sql:Error result = dbClient->queryRow(`SELECT id, year, month, day, time 
                                                    FROM international_time_slots 
                                                    WHERE time = ${request.preferredTimeSlot} AND is_available = true 
                                                    ORDER BY id LIMIT 1`);
    if result is sql:NoRowsError {
        io:println("No available time slots for the preferred time.");
        return error("No available time slots.");
    } else if result is sql:Error {
        return result;
    } else {
        int slotId = result.id;
        string timeSlot = result.time;
        string trackingId = check generateTrackingId();
        string estimatedDeliveryTime = check calculateEstimatedDeliveryTime(result.year, result.month, result.day, timeSlot);
        string pickup_time = string `${result.year}-${result.month}-${result.day} time: ${timeSlot}`;

        // Update the time slot to unavailable
        sql:ExecutionResult|sql:Error updateResult = dbClient->execute(`UPDATE international_time_slots 
                                                                         SET is_available = false WHERE id = ${slotId}`);
        if updateResult is sql:Error {
            return updateResult;
        }

        // Insert the delivery details into the database
        sql:ExecutionResult|sql:Error insertResult = dbClient->execute(`INSERT INTO international_deliveries 
                                                                        (tracking_id, customer_id, first_name, last_name, 
                                                                         contact_number, shipment_type, pickup_location, 
                                                                         delivery_location, pickup_time_id, estimated_delivery_time, 
                                                                         customs_info) 
                                                                        VALUES (${trackingId}, ${request.id}, ${request.firstName}, 
                                                                        ${request.lastName}, ${request.contactNumber}, 
                                                                        ${request.shipmentType}, ${request.pickupLocation}, 
                                                                        ${request.deliveryLocation}, ${slotId}, 
                                                                        ${estimatedDeliveryTime}, 'Customs details for shipping to ${request.country}')`);
        if insertResult is sql:Error {
            return insertResult;
        }

        // Send confirmation back to logistics service
        json payload = {
            tracking_id: trackingId,
            requestID: request.id,
            firstName: request.firstName,
            lastName: request.lastName,
            contactNumber: request.contactNumber,
            shipmentType: request.shipmentType,
            pickupLocation: request.pickupLocation,
            deliveryLocation: request.deliveryLocation,
            pickup_time: pickup_time,
            estimated_delivery_time: estimatedDeliveryTime,
            customs_info: "Customs details for shipping to " + request.country
        };
        check kafkaProducer->send({topic: "international-delivery-response", value: payload.toString()});
        io:println("International delivery confirmed with tracking ID: ", trackingId);
    }
}

// Function to generate a random tracking ID
function generateTrackingId() returns string|random:Error {
    int randomNum = check random:createIntInRange(100000, 999999);
    return "TRK-" + randomNum.toString();
}

// Function to calculate the estimated delivery time
function calculateEstimatedDeliveryTime(int year, int month, int day, string timeSlot) returns string|error {
    int nextDay = day + 1; // Assume next day for simplicity; adjust based on your logic
    return string `${year}-${month}-${nextDay}T10:00`; // Adjust the time accordingly
}
