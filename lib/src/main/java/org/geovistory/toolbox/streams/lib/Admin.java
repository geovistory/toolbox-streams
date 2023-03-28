package org.geovistory.toolbox.streams.lib;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class Admin {

    public void createOrConfigureTopics(ArrayList<String> topicNames, Integer numPartitions, short replicationFactor) {


        try (AdminClient adminClient = AdminClient.create(getAdminConfig())) {

            var config = getTopicConfig();

            topicNames.forEach(topicName -> {
                var n = new NewTopic(topicName, numPartitions, replicationFactor);
                n.configs(config);

                System.out.println("Creating topic " + topicName);

                var future = adminClient.createTopics(List.of(n)).all();

                try {
                    future.get();
                    System.out.println("> Topic " + topicName + " created");

                } catch (InterruptedException | ExecutionException e) {
                    if (e.getCause() instanceof TopicExistsException) {
                        System.out.println("> Topic " + topicName + " already exists");
                        configureTopic(topicName, config);
                    } else {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
    }

    public void configureTopic(String topicName, Map<String, String> config) {
        System.out.println("> Configuring topic " + topicName);
        try (AdminClient adminClient = AdminClient.create(getAdminConfig())) {

            ConfigResource topicRes = new ConfigResource(ConfigResource.Type.TOPIC, topicName);

            List<AlterConfigOp> alt = new ArrayList<>();
            for (var param : config.entrySet()) {
                AlterConfigOp alterConfigOp = new AlterConfigOp(
                        new ConfigEntry(param.getKey(), param.getValue()),
                        AlterConfigOp.OpType.SET);
                alt.add(alterConfigOp);
            }


            final Map<ConfigResource, Collection<AlterConfigOp>> configsMap = new HashMap<>();
            configsMap.put(topicRes, alt);

            adminClient.incrementalAlterConfigs(configsMap).all().get();

        } catch (ExecutionException | InterruptedException e) {
            System.out.println("> Error configuring topic " + topicName);
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        System.out.println("> Configured topic " + topicName);

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
