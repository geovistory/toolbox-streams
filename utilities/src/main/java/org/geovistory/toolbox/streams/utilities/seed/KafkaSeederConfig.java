package org.geovistory.toolbox.streams.utilities.seed;

import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import io.apicurio.registry.serde.strategy.TopicIdStrategy;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.geovistory.toolbox.streams.lib.AppConfig;

import java.util.Properties;

public class KafkaSeederConfig {

    public static String fooTopicName = "testfoo";

    public static Properties getConsumerConfig(String TOPIC_NAME) {
        Properties config = new Properties();

        // Configure the client with the URL of Apicurio Registry
        config.putIfAbsent(SerdeConfig.REGISTRY_URL, AppConfig.INSTANCE.getApicurioRegistryUrl());

        // Configure Kafka settings
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, AppConfig.INSTANCE.getKafkaBootstrapServers());
        config.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "Consumer-" + TOPIC_NAME);
        config.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        config.putIfAbsent(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        config.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Configure the client with the serializer, and the strategy to look up the schema in Apicurio Registry
        config.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                AvroKafkaDeserializer.class.getName());
        config.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                AvroKafkaDeserializer.class.getName());

        return config;
    }
    public static Properties getProducerConfig() {
        Properties config = new Properties();

        // Configure the client with the URL of Apicurio Registry
        config.putIfAbsent(SerdeConfig.REGISTRY_URL, AppConfig.INSTANCE.getApicurioRegistryUrl());

        // Configure the client with the serializer, and the strategy to look up the schema in Apicurio Registry
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, AppConfig.INSTANCE.getKafkaBootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, AvroKafkaSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroKafkaSerializer.class.getName());

        // Default strategy that uses the topic name and key or value suffix.
        config.put(SerdeConfig.ARTIFACT_RESOLVER_STRATEGY, TopicIdStrategy.class.getName());

        // Specify whether the serializer tries to create an artifact in the registry.
        config.put(SerdeConfig.AUTO_REGISTER_ARTIFACT, Boolean.TRUE);

        // https://access.redhat.com/documentation/en-us/red_hat_integration/2022.q1/html/service_registry_user_guide/using-kafka-client-serdes#registry-serdes-concepts-strategy-registry
        config.put(SerdeConfig.FIND_LATEST_ARTIFACT, Boolean.FALSE);
        return config;
    }
}
