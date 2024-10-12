-- Create the customer_requests table (ID not auto-increment)
CREATE TABLE customer_requests (
    id VARCHAR(60) PRIMARY KEY,
    -- Ensure this column is not auto-increment
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    contact_number VARCHAR(15),
    shipment_type VARCHAR(20),
    pickup_location VARCHAR(100),
    delivery_location VARCHAR(100),
    preferred_time_slot VARCHAR(50),
    timestamp DATETIME
);
-- Create the standard_time_slots table
-- Create the standard_time_slots table
CREATE TABLE standard_time_slots (
    id INT AUTO_INCREMENT PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    time VARCHAR(10),
    is_available BOOLEAN DEFAULT TRUE
);
-- Create the standard_deliveries table with customer_id as a foreign key
CREATE TABLE standard_deliveries (
    id INT AUTO_INCREMENT PRIMARY KEY,
    tracking_id VARCHAR(60),
    customer_id VARCHAR(60),
    -- Foreign key referring to customer_requests
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    contact_number VARCHAR(20),
    shipment_type VARCHAR(20),
    pickup_location VARCHAR(100),
    delivery_location VARCHAR(100),
    pickup_time_id INT,
    -- Foreign key referring to standard_time_slots
    estimated_delivery_time DATETIME,
    status VARCHAR(20) DEFAULT 'pending',
    FOREIGN KEY (customer_id) REFERENCES customer_requests(id),
    FOREIGN KEY (pickup_time_id) REFERENCES standard_time_slots(id)
);