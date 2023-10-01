FROM openjdk:11

# Install Maven
RUN apt-get update && \
    apt-get -y install maven && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy the Maven project files to the Docker image
COPY pom.xml .

RUN mvn dependency:go-offline

COPY src ./src

# Build the Maven project
RUN mvn package