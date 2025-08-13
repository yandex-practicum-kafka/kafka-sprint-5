package com.example.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component // Помечает класс как Spring Component, позволяя Spring управлять его жизненным циклом и внедрять зависимости
public class MessageListener {

    // @KafkaListener аннотация указывает, что этот метод должен слушать сообщения из Kafka топика
    // topics = "${kafka.topic1}" - указывает, из какого топика читать сообщения. Значение берется из application.yml (kafka.topic1 = topic-1)
    // groupId = "${spring.kafka.consumer.group-id}" - указывает ID группы потребителей. Важно для масштабирования и отказоустойчивости.

    // Сообщения из topic-1 будут распределены между всеми потребителями в этой группе.  Значение берется из application.yml (spring.kafka.consumer.group-id = group_id)
    @KafkaListener(topics = "${kafka.topic1}", groupId = "${spring.kafka.consumer.group-id}")
    public void listenTopic1(String message) {
        // Этот метод вызывается каждый раз, когда в топик topic-1 приходит новое сообщение
        System.out.println("Received Message from Topic 1: " + message); // Выводим полученное сообщение в консоль
    }

    // @KafkaListener аннотация указывает, что этот метод должен слушать сообщения из Kafka топика
    // topics = "${kafka.topic2}" - указывает, из какого топика читать сообщения. Значение берется из application.yml (kafka.topic2 = topic-2)
    // groupId = "${spring.kafka.consumer.group-id}" - указывает ID группы потребителей.

    // Важно: Согласно заданию, консьюмеры *не* должны иметь доступа к чтению из topic-2.
    // Однако, этот код позволяет консьюмерам читать из topic-2.  **ЭТО ПРОТИВОРЕЧИТ ТРЕБОВАНИЯМ ЗАДАЧИ.**
    // Для реализации ограничения доступа необходимо настраивать ACL (Access Control Lists) в Kafka.
    // Простое удаление этого метода *недостаточно*, т.к. потребители все равно смогут подписаться на этот топик, если у них есть соответствующие права в Kafka.
    @KafkaListener(topics = "${kafka.topic2}", groupId = "${spring.kafka.consumer.group-id}")
    public void listenTopic2(String message) {
        // Этот метод вызывается каждый раз, когда в топик topic-2 приходит новое сообщение
        System.out.println("Received Message from Topic 2: " + message); // Выводим полученное сообщение в консоль
    }
}
