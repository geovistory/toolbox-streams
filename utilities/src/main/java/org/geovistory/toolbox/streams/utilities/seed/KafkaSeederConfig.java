package org.geovistory.toolbox.streams.utilities.seed;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.geovistory.toolbox.streams.lib.AppConfig;

import java.util.Properties;

public class KafkaSeederConfig {

    public static String fooTopicName = "testfoo";

    public static Properties getConsumerConfig(String TOPIC_NAME) {
        Properties config = new Properties();

        // Configure Kafka settings
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, AppConfig.INSTANCE.getKafkaBootstrapServers());
        config.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "Consumer-3-" + TOPIC_NAME);
        config.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        config.putIfAbsent(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        config.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");



        return config;
    }
    public static Properties getProducerConfig() {
        Properties config = new Properties();


        // Configure the client with the serializer, and the strategy to look up the schema in Apicurio Registry
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, AppConfig.INSTANCE.getKafkaBootstrapServers());

        return config;
    }
}
