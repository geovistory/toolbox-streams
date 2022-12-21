package org.geovistory.toolbox.streams.app;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.geovistory.toolbox.streams.lib.AppConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Admin {

    public String createTopic(String topicName, Integer numPartitions) {
        try (AdminClient adminClient = AdminClient.create(getAdminConfig())) {
            NewTopic topic = new NewTopic(topicName, numPartitions, (short) 1);
            topic.configs(getTopicConfig());

            adminClient.createTopics(Collections.singletonList(topic));

        }
        return topicName;
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
