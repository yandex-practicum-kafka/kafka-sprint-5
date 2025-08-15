#!/bin/bash

# Определяем имя темы
TOPIC_NAME="balanced_topic"

# Проверка существования темы
if /opt/bitnami/kafka/bin/kafka-topics.sh --describe --topic "$TOPIC_NAME" --bootstrap-server kafka-0:9092; then
    echo "Topic '$TOPIC_NAME' already exists."
else
    echo "Topic '$TOPIC_NAME' does not exist. Creating the topic..."
    # Создаем новую тему, если она не существует
    /opt/bitnami/kafka/bin/kafka-topics.sh --create --topic "$TOPIC_NAME" --partitions 8 --replication-factor 3 --bootstrap-server kafka-0:9092
    echo "Topic '$TOPIC_NAME' created successfully. Here is the topic description:"
    # Выводим описание вновь созданной темы
    /opt/bitnami/kafka/bin/kafka-topics.sh --describe --topic "$TOPIC_NAME" --bootstrap-server kafka-0:9092
fi
