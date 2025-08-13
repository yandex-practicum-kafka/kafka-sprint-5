package com.example.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;

@EnableKafka // Включает поддержку Kafka в Spring
@Configuration // Помечает класс как источник бинов для Spring IoC контейнера
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}") // Получает значение из application.yml
    private String bootstrapServers; // Адреса Kafka брокеров

    @Value("${spring.kafka.security.protocol}") // Получает значение из application.yml
    private String securityProtocol; // Протокол безопасности (SSL, SASL_SSL, и т.д.)

    @Value("${spring.kafka.ssl.key-store-location}") // Получает значение из application.yml
    private String keyStoreLocation; // Путь к файлу keystore

    @Value("${spring.kafka.ssl.key-store-password}") // Получает значение из application.yml
    private String keyStorePassword; // Пароль keystore

    @Value("${spring.kafka.ssl.trust-store-location}") // Получает значение из application.yml
    private String trustStoreLocation; // Путь к файлу truststore

    @Value("${spring.kafka.ssl.trust-store-password}") // Получает значение из application.yml
    private String trustStorePassword; // Пароль truststore

    @Value("${spring.kafka.consumer.group-id}") // Получает значение из application.yml
    private String groupId; // ID группы потребителей

    @Value("${spring.kafka.consumer.auto-offset-reset}") // Получает значение из application.yml
    private String autoOffsetReset; // Политика сброса смещения (earliest, latest, none)


    // Consumer Configuration
    @Bean // Создает бин ConsumerFactory
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>(); // Создаем Map для хранения свойств конфигурации

        // Настраиваем основные свойства Consumer
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers); // Адреса Kafka брокеров
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId); // ID группы потребителей
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class); // Десериализатор ключа
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class); // Десериализатор значения
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset); // Политика сброса смещения

        // Настраиваем свойства безопасности (SSL)
        props.put("security.protocol", securityProtocol); // Протокол безопасности (можно использовать ConsumerConfig.SECURITY_PROTOCOL_CONFIG, если securityProtocol - константа)
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStoreLocation); // Путь к truststore
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustStorePassword); // Пароль truststore
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStoreLocation); // Путь к keystore
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keyStorePassword); // Пароль keystore

        return new DefaultKafkaConsumerFactory<>(props); // Создаем и возвращаем ConsumerFactory
    }

    @Bean // Создает бин ConcurrentKafkaListenerContainerFactory
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>(); // Создаем фабрику
        factory.setConsumerFactory(consumerFactory()); // Устанавливаем ConsumerFactory
        return factory; // Возвращаем фабрику
    }

    // Producer Configuration
    @Bean // Создает бин ProducerFactory
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>(); // Создаем Map для хранения свойств конфигурации

        // Настраиваем основные свойства Producer
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers); // Адреса Kafka брокеров
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class); // Сериализатор ключа
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class); // Сериализатор значения

        // Настраиваем свойства безопасности (SSL)
        configProps.put("security.protocol", securityProtocol); // Протокол безопасности (можно использовать ProducerConfig.SECURITY_PROTOCOL_CONFIG, если securityProtocol - константа)
        configProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStoreLocation); // Путь к truststore
        configProps.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustStorePassword); // Пароль truststore
        configProps.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStoreLocation); // Путь к keystore
        configProps.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keyStorePassword); // Пароль keystore

        return new DefaultKafkaProducerFactory<>(configProps); // Создаем и возвращаем ProducerFactory
    }

    @Bean // Создает бин KafkaTemplate
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory()); // Создаем и возвращаем KafkaTemplate, используя ProducerFactory
    }
}
