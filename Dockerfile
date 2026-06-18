# Stage 1: Build
FROM maven:3.9-eclipse-temurin-17 AS builder

WORKDIR /build

COPY pom.xml .
RUN mvn dependency:resolve

COPY . .
RUN mvn clean package -DskipTests

# Stage 2: Runtime
FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

COPY --from=builder /build/target/dispatch-consumer-*.jar app.jar

# Variables de entorno con valores por defecto (para docker-compose)
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV SCHEMA_REGISTRY_URL=http://schema-registry:8081
ENV MONGO_URI=mongodb://mongodb:27017/cardsdb
ENV REDIS_HOST=redis
ENV REDIS_PORT=6379
ENV SERVER_PORT=8086

EXPOSE ${SERVER_PORT}

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD wget -q -O- http://localhost:${SERVER_PORT}/api/health || exit 1

ENTRYPOINT ["java", "-jar", "app.jar"]
