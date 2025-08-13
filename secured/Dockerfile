# Этап сборки приложения
FROM openjdk:21-jdk-slim AS builder

WORKDIR /app

# Копируем файлы Gradle
COPY gradle gradle
COPY gradlew gradlew
COPY settings.gradle settings.gradle
COPY build.gradle build.gradle

# Копируем исходный код
COPY src src

# Запускаем сборку Gradle
RUN ./gradlew clean build -x test

# Этап запуска приложения
FROM openjdk:21-jdk-slim

WORKDIR /app

# Копируем собранный JAR файл
COPY --from=builder /app/build/libs/KafkaApp-0.0.1-SNAPSHOT.jar app.jar

# Устанавливаем необходимые системы и библиотеки для работы с SSL
RUN apt-get update && apt-get install -y \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

ENTRYPOINT java $JAVA_OPTS -Djavax.net.ssl.keyStore=/app/clients-creds/kafka_user.keystore.jks -Djavax.net.ssl.keyStorePassword=changeit -Djavax.net.ssl.trustStore=/app/clients-creds/kafka_user.truststore.jks -Djavax.net.ssl.trustStorePassword=changeit -Djava.security.egd=file:/dev/.urandom -jar app.jar