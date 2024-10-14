import ballerina/io;
import ballerina/random;
import ballerinax/kafka;

// Initialize Kafka Producer
kafka:Producer kafkaProducer = check new (kafka:DEFAULT_URL, {
    clientId: "logistics_producer",
    acks: "all"
});

// Define the structure of the customer request
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

// Kafka Consumer for handling responses
kafka:Consumer Logisticsresponses = check new (kafka:DEFAULT_URL, {
    groupId: "logistics_group",
    topics: ["Logistics-service-response"]
});

public function main() returns error? {
    // Get user input
    io:println("Enter first name:");
    string firstName = io:readln();
    io:println("Enter last name:");
    string lastName = io:readln();
    io:println("Enter contact number:");
    string contactNumber = io:readln();

    // Shipment Type Selection
    string shipmentType = getShipmentType();

    io:println("Enter pickup location:");
    string pickupLocation = io:readln();
    io:println("Enter delivery location:");
    string deliveryLocation = io:readln();

    // Time Slot Selection
    string preferredTimeSlot = getTimeSlot();

    // Generate a random request ID
    int randomNum = check random:createIntInRange(100000, 999999);
    string strRandomNum = randomNum.toString();

    // Create customer request
    CustomerRequest customerRequest = {
        id: strRandomNum,
        firstName: firstName,
        lastName: lastName,
        contactNumber: contactNumber,
        shipmentType: shipmentType,
        pickupLocation: pickupLocation,
        deliveryLocation: deliveryLocation,
        preferredTimeSlot: preferredTimeSlot
    };

    // Handle customer request and send to Kafka
    check handleCustomerRequest(customerRequest, customerRequest.id);
    io:println("Customer request sent to logistics service.");
}

// Function to handle customer request
function handleCustomerRequest(CustomerRequest customerRequest, string customer_Id) returns error? {
    // Send customer request to Kafka topic
    check kafkaProducer->send({
        topic: "new-delivery-requests",
        value: customerRequest.toJsonString()
    });

    // Handle the response from Kafka
    CustomerResponse response = check handleResponses(customer_Id);
    io:println("Response loading........");
    displayResponse(response);
}

// Function to display the response
function displayResponse(CustomerResponse response) {
    io:println("------------------------------------------------");
    io:println("Your information:");
    io:println(string `First Name: ${response.firstName}`);
    io:println(string `Last Name: ${response.lastName}`);
    io:println(string `Contact Number: ${response.contactNumber}`);
    io:println("------------------------------------------------");
    io:println("Delivery information:");
    io:println(string `Shipment Type: ${response.shipmentType}`);
    io:println(string `Pickup Location: ${response.pickupLocation}`);
    io:println(string `Delivery Location: ${response.deliveryLocation}`);
    io:println(string `Pickup Time: ${response.pickup_time}`);
    io:println(string `Tracking ID: ${response.tracking_id}`);
    io:println(string `Estimated Delivery Time: ${response.estimated_delivery_time}`);
    io:println("------------------------------------------------");
    io:println(`THANK YOU FOR USING OUR DELIVERY SERVICE, ${response.firstName}`);
}

// Function to handle Kafka responses
function handleResponses(string customer_ID) returns CustomerResponse|error {
    CustomerResponse[] responses = check Logisticsresponses->pollPayload(1000);

    foreach var response in responses {
        if response.customer_id.toString() == customer_ID.toString() {
            return response;
        }
    }
    return error("No matching response found for requestID: " + customer_ID);
}

// Function to get shipment type
function getShipmentType() returns string {
    io:println("Select shipment type:");
    io:println("1: Standard");
    io:println("2: Express");
    io:println("3: International");

    string[] shipmentTypes = ["standard", "express", "international"];
    string choiceInput = io:readln();
    int|error choice = int:fromString(choiceInput);

    if choice is int && choice >= 1 && choice <= 3 {
        return shipmentTypes[choice - 1];
    } else {
        io:println("Invalid choice. Please choose again.");
        return getShipmentType();
    }
}

// Function to get available time slots in 24-hour format
function getTimeSlot() returns string {
    io:println("Available time slots:");

    string[] timeSlots = getTimeSlots();
    foreach int i in 0 ..< (timeSlots.length()) {
        io:println(string `${i + 1}: ${timeSlots[i]}`);
    }

    io:println("Select a time slot by entering the number:");
    string choiceInput = io:readln();
    int|error choice = int:fromString(choiceInput);

    if choice is int && choice >= 1 && choice <= timeSlots.length() {
        return timeSlots[choice - 1];
    } else {
        io:println("Invalid choice. Please choose again.");
        return getTimeSlot();
    }
}

// Function to define available time slots (08:00 - 17:00 in 24-hour format)
function getTimeSlots() returns string[] {
    return [
        "08:00 - 09:00",
        "09:00 - 10:00",
        "10:00 - 11:00",
        "11:00 - 12:00",
        "12:00 - 13:00",
        "13:00 - 14:00",
        "14:00 - 15:00",
        "15:00 - 16:00",
        "16:00 - 17:00"
    ];
}
