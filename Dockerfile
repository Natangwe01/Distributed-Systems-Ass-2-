# Shared by all services. Build context is the service folder (see docker-compose.yml).
FROM ballerina/ballerina:2201.10.0 AS build
USER root
WORKDIR /app
COPY . .
RUN bal build

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/target/bin/*.jar /app/service.jar
ENTRYPOINT ["java", "-jar", "/app/service.jar"]
