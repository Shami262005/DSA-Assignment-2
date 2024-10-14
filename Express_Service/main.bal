import ballerina/io;
import ballerina/random;
import ballerina/sql;
import ballerinax/kafka;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;

// Define the delivery request record
type DeliveryRequest record {
    string id;
    string firstName;
    string lastName;
    string contactNumber;
    string pickupLocation;
    string shipmentType; // This will capture standard, express, or international
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

// Kafka listener for receiving delivery requests
listener kafka:Listener kafkaListener = new (kafka:DEFAULT_URL, {
    groupId: "express_delivery_group",
    topics: ["express-delivery-request"]
});

// Kafka producer to send responses back
kafka:Producer kafkaProducer = check new (kafka:DEFAULT_URL);

// MySQL client setup
mysql:Client dbClient = check new (user = "root", password = "Victoria@1509", database = "Assignment2", host = "localhost", port = 3306);

// Kafka service that listens for incoming express delivery requests
service on kafkaListener {

    remote function onConsumerRecord(DeliveryRequest[] requests) returns kafka:Error? {
        foreach var request in requests {
            io:println(string `Received ${request.shipmentType} delivery request from ${request.firstName} ${request.lastName}`);
            var result = processExpressDeliveryRequest(request);
            if (result is error) {
                io:println("Error processing request: ", result.message());
            } else {
                io:println("Successfully processed the express delivery request.");
            }
        }
    }
}

// Function to process each express delivery request
function processExpressDeliveryRequest(DeliveryRequest request) returns error? {
    io:println("Processing express delivery request for: ", request.firstName, " ", request.lastName);
    io:println("Preferred time slot: ", request.preferredTimeSlot);

    // Check for valid shipment type
    if (request.shipmentType != "express") {
        io:println("Request is not for express delivery.");
        return error("Invalid shipment type; only express delivery is accepted.");
    }

    // Check available time slots for express delivery
    Timeslot|sql:Error result = dbClient->queryRow(`SELECT id, year, month, day, time 
                                                    FROM express_time_slots 
                                                    WHERE time = ${request.preferredTimeSlot} AND is_available = true 
                                                    ORDER BY id LIMIT 1`);
    if result is sql:NoRowsError {
        io:println("No available time slots for express delivery.");
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
        sql:ExecutionResult|sql:Error updateResult = dbClient->execute(`UPDATE express_time_slots 
                                                                     SET is_available = false WHERE id = ${slotId}`);
        if updateResult is sql:Error {
            return updateResult;
        }

        // Insert the express delivery details into the database
        sql:ExecutionResult|sql:Error insertResult = dbClient->execute(`INSERT INTO express_deliveries 
                                                                     (tracking_id, customer_id, first_name, last_name, 
                                                                      contact_number, shipment_type, pickup_location, 
                                                                      delivery_location, pickup_time_id, estimated_delivery_time) 
                                                                      VALUES (${trackingId}, ${request.id}, ${request.firstName}, 
                                                                      ${request.lastName}, ${request.contactNumber}, 
                                                                      ${request.shipmentType}, ${request.pickupLocation}, 
                                                                      ${request.deliveryLocation}, ${slotId}, 
                                                                      ${estimatedDeliveryTime})`);
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
            estimated_delivery_time: estimatedDeliveryTime
        };
        check kafkaProducer->send({topic: "express-delivery-response", value: payload.toString()});
        io:println("Express delivery confirmed with tracking ID: ", trackingId);
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
