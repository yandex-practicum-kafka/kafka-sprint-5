package com.example.kafka.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Value;

@Component // Помечает класс как Spring Component, позволяя Spring управлять его жизненным циклом и внедрять зависимости
public class MessageProducer {

    private final KafkaTemplate<String, String> kafkaTemplate; // KafkaTemplate - это шаблон для отправки сообщений в Kafka

    @Value("${kafka.topic1}") // Читает значение из application.yml для ключа kafka.topic1
    private String topic1; // Имя первого топика Kafka

    @Value("${kafka.topic2}") // Читает значение из application.yml для ключа kafka.topic2
    private String topic2; // Имя второго топика Kafka

    // Внедрение KafkaTemplate через конструктор (рекомендуемый способ)
    public MessageProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate; // Инициализируем kafkaTemplate переданным экземпляром
    }

    @Scheduled(fixedDelayString = "${message.production-interval}") //  Помечает метод как задачу, выполняемую по расписанию.
    //  fixedDelayString = "${message.production-interval}" - указывает интервал между выполнениями задачи в миллисекундах. Значение берется из application.yml (message.production-interval = 500)
    public void sendMessages() {
        String message1 = "Message to Topic 1"; // Текст сообщения для первого топика
        String message2 = "Message to Topic 2"; // Текст сообщения для второго топика

        kafkaTemplate.send(topic1, message1); // Отправляем сообщение в первый топик
        kafkaTemplate.send(topic2, message2); // Отправляем сообщение во второй топик

        System.out.println("Produced: " + message1 + " to " + topic1); // Выводим информацию об отправке сообщения в консоль
        System.out.println("Produced: " + message2 + " to " + topic2); // Выводим информацию об отправке сообщения в консоль
    }
}

