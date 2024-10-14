FROM ballerina/ballerina:latest  

# Set the working directory in the container
WORKDIR /app

# Copy the Ballerina project files to the container
COPY . .

# Switch to root user to ensure permissions
USER root

# Build the Ballerina project
RUN bal build

# Expose the port that the Ballerina service will use
EXPOSE 9090

# Define the entry point for the Docker container
CMD ["java", "-jar", "target/bin/International_Service.jar"]
