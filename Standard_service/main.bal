import ballerina/io;
import ballerina/random;
import ballerina/sql;
import ballerinax/kafka;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;

// Define the standard delivery request record
type StandardDeliveryRequest record {
    string customerID;
    string firstName;
    string lastName;
    string contactNumber;
    string pickupLocation;
    string shipmentType;
    string deliveryLocation;
    string preferredTimeSlot;
};

type standardresponses record {
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

type Timeslot record {
    int id;
    int year;
    int month;
    int day;
    string time;
};

// Kafka listener for receiving standard delivery requests
listener kafka:Listener kafkaListener = new (kafka:DEFAULT_URL, {
    groupId: "standard_delivery_group",
    topics: ["standard-delivery-requests"]
});

// Kafka producer to send responses back
kafka:Producer kafkaProducer = check new (kafka:DEFAULT_URL);

// MySQL client setup
mysql:Client dbClient = check new (user = "root", password = "Victoria@1509", database = "logistics_service", host = "localhost", port = 3306);

// Kafka service that listens for incoming standard delivery requests
service on kafkaListener {

    remote function onConsumerRecord(StandardDeliveryRequest[] requests) returns kafka:Error? {
        foreach var request in requests {
            io:println(string `Received standard delivery request from ${request.firstName} ${request.lastName}`);
            var Result = processStandardDeliveryRequest(request);
            if (Result is error) {
                io:println("Error processing request: ", Result.message());
            } else {
                io:println("Successfully processed the standard delivery request.");
            }
        }
    }
}

// Function to process each standard delivery request
function processStandardDeliveryRequest(StandardDeliveryRequest request) returns error? {
    io:println("Processing delivery request for: ", request.firstName, " ", request.lastName);
    io:println("Preferred time slot: ", request.preferredTimeSlot);

    Timeslot|sql:Error result = dbClient->queryRow(`SELECT id, year, month, day, time 
                                                    FROM standard_time_slots 
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
        string year = result.year.toString();
        string month = result.month.toString();
        string day = result.day.toString();

        string pickup_time = (year + "-" + month + "-" + "-" + day + " time:" + timeSlot);
        // Update the time slot to unavailable
        sql:ExecutionResult|sql:Error updateResult = dbClient->execute(`UPDATE standard_time_slots 
                                                                         SET is_available = false WHERE id = ${slotId}`);
        if updateResult is sql:Error {
            return updateResult;
        }
        string status = "Confirmed";

        // Insert the delivery details into the database
        sql:ExecutionResult|sql:Error insertResult = dbClient->execute(`INSERT INTO standard_deliveries 
                                                                        (tracking_id, customer_id, first_name, last_name, contact_number,shipment_type,
                                                                         pickup_location, delivery_location, pickup_time_id, 
                                                                         estimated_delivery_time,status) 
                                                                         VALUES (${trackingId},${request.customerID} ,${request.firstName}, 
                                                                         ${request.lastName}, ${request.contactNumber},${request.shipmentType},
                                                                         ${request.pickupLocation}, ${request.deliveryLocation}, 
                                                                         ${slotId}, ${estimatedDeliveryTime},${status})`);
        if insertResult is sql:Error {
            return insertResult;
        }

        // Send confirmation back to logistics service
        json payload = {
            tracking_id: trackingId,
            customer_id: request.customerID,
            firstName: request.firstName,
            lastName: request.lastName,
            contactNumber: request.contactNumber,
            shipmentType: request.shipmentType,
            pickupLocation: request.pickupLocation,
            deliveryLocation: request.deliveryLocation,
            pickup_time: pickup_time,
            estimated_delivery_time: estimatedDeliveryTime
        };
        check kafkaProducer->send({topic: "standard-delivery-response", value: payload.toString()});
        io:println("Standard delivery confirmed with tracking ID: ", trackingId);
    }

}

// Function to generate a random tracking ID
function generateTrackingId() returns string|random:Error {
    int randomNum = check random:createIntInRange(100000, 999999);
    return "TRK-" + randomNum.toString();
}

// Function to calculatbe the estimated delivery time (2 hours ahead of the last entry)
function calculateEstimatedDeliveryTime(int year, int month, int day, string timeSlot) returns string|error {

    int nextDay = day + 1;
    // Construct the new estimated delivery time string (ISO 8601 format: YYYY-MM-DDTHH:mm)
    return string `${year}-${month}-${nextDay}T10:00`;
}

