
services:
  # --- INFRASTRUCTURE: KAFKA (KRAFT MODE) ---
  kafka:
    image: apache/kafka:latest
    container_name: kafka
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    ports:
      - "9092:9092"

  # --- BACKEND: ORDER SERVICE (PRODUCER) ---
  order-service:
    build: ./order-service
    container_name: order-service
    ports:
      - "5000:5000"
    environment:
      KAFKA_BROKER: kafka:9092
    depends_on:
      - kafka

  # --- BACKEND: SSE SERVICE (DISPATCHER) ---
  sse-service:
    build: ./sse-service
    container_name: sse-service
    ports:
      - "5001:5001"
    environment:
      KAFKA_BROKER: kafka:9092
    depends_on:
      - kafka

  # --- FRONTEND: REACT CLIENT ---
  react-app:
    build: ./react-client
    container_name: react-app
    ports:
      - "3000:3000"
    stdin_open: true # Required for React dev server
    environment:
      - CHOKIDAR_USEPOLLING=true # Helps with file watching in Docker