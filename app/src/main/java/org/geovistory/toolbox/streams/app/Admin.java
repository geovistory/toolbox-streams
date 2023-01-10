package org.geovistory.toolbox.streams.app;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.TopicConfig;
import org.geovistory.toolbox.streams.lib.AppConfig;

import java.util.*;

public class Admin {

    public KafkaFuture<Void> createTopics(String[] topicNames, Integer numPartitions, short replicationFactor) {

        KafkaFuture<Void> future;

        try (AdminClient adminClient = AdminClient.create(getAdminConfig())) {
            var config = getTopicConfig();
            var newTopics = new ArrayList<NewTopic>();
            Arrays.stream(topicNames).forEach(topicName -> {
                var n = new NewTopic(topicName, numPartitions, replicationFactor);
                n.configs(config);
                newTopics.add(n);
            });
            future = adminClient.createTopics(newTopics).all();
        }
        return future;
    }

    private static Properties getAdminConfig() {
        AppConfig appConfig = AppConfig.INSTANCE;

        // set the required properties for running Kafka Streams
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.getKafkaBootstrapServers());

        return props;
    }

    public static Map<String, String> getTopicConfig() {
        Map<String, String> configMap = new HashMap<>();
        configMap.put(TopicConfig.RETENTION_MS_CONFIG, "-1");
        configMap.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        return configMap;
    }

}
