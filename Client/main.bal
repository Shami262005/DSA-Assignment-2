import ballerina/io;
import ballerina/random;
import ballerinax/kafka;

// Initialize Kafka Producer
kafka:Producer kafkaProducer = check new(kafka:DEFAULT_URL, {
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
    io:println("Enter shipment type (standard/express/international):");
    string shipmentType = io:readln();
    io:println("Enter pickup location:");
    string pickupLocation = io:readln();
    io:println("Enter delivery location:");
    string deliveryLocation = io:readln();

    // Show available time slots
    io:println("Available time slots:");
    string[] timeSlots = getTimeSlots();
    foreach string slot in timeSlots {
        io:println(slot);
    }
    io:println("Enter preferred time slot:");
    string preferredTimeSlot = io:readln();

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

// Function to get available time slots
function getTimeSlots() returns string[] {
    return ["07:00 AM - 09:00 AM", "11:00 AM - 01:00 PM", "03:00 PM - 05:00 PM"];
}
