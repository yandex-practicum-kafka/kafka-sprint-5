package com.example.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component // Помечает класс как Spring Component, позволяя Spring управлять его жизненным циклом и внедрять зависимости
public class MessageListener {

    // @KafkaListener аннотация указывает, что этот метод должен слушать сообщения из Kafka топика
    // topics = "${kafka.topic1}" - указывает, из какого топика читать сообщения. Значение берется из application.yml (kafka.topic1 = topic-1)
    // groupId = "${spring.kafka.consumer.group-id}" - указывает ID группы потребителей. Важно для масштабирования и отказоустойчивости.

    // Сообщения из topic-1 будут распределены между всеми потребителями в этой группе.  Значение берется из application.yml (spring.kafka.consumer.group-id = group_id)

    // Слушаем только topic-1 (т.к. консьюмеры должны иметь доступ только к topic-1).
    @KafkaListener(topics = "${kafka.topic1}", groupId = "${spring.kafka.consumer.group-id}")
    public void listenTopic1(String message) {
        // Этот метод вызывается каждый раз, когда в топик topic-1 приходит новое сообщение
        System.out.println("Received Message from Topic 1: " + message); // Выводим полученное сообщение в консоль
    }

    // @KafkaListener аннотация указывает, что этот метод должен слушать сообщения из Kafka топика
    // topics = "${kafka.topic2}" - указывает, из какого топика читать сообщения. Значение берется из application.yml (kafka.topic2 = topic-2)
    // groupId = "${spring.kafka.consumer.group-id}" - указывает ID группы потребителей.

    // ВАЖНО: слушатель для topic-2 удалён, чтобы не нарушать требование «консьюмеры не имеют доступа к чтению topic-2».
    
    // Для отладки добавлен код-скелет, включаемый через property (enable-topic2-consumer), для быстрого включения потребителя на topic-2

    // @ConditionalOnProperty(prefix = "kafka", name = "enable-topic2-consumer", havingValue = "true")
    // @KafkaListener(topics = "${kafka.topic2}", groupId = "${spring.kafka.consumer.group-id}")
    // public void listenTopic2(String message) {
    //     System.out.println("Received Message from Topic 2: " + message);
    // }

    // При включения потребителя topic-2 для работы слушателя необходимо явно разрешать чтение 
    // (правило READ на topic-2 в Kafka ACL), иначе даже включённый слушатель не будет получать сообщения
}
