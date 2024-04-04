package org.geovistory.toolbox.streams.entity.label3.lib;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.streams.KeyValue;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.entity.label3.names.InputTopicNames;
import org.geovistory.toolbox.streams.entity.label3.names.OutputTopicNames;

import java.util.ArrayList;
import java.util.List;


@ApplicationScoped
public class TopicsCreator {

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrapServers;
    @ConfigProperty(name = "ts.output.topic.partitions")
    int outputTopicPartitions;
    @ConfigProperty(name = "ts.output.topic.replication.factor")
    short outputTopicReplicationFactor;

    @Inject
    InputTopicNames inputTopicNames;

    @Inject
    OutputTopicNames outputTopicNames;

    /**
     * Creates the input topics with the desired configuration.
     * Use this in test before seeding data into input topics
     * to ensure they have the same number of partitions as
     * the output topics.
     */
    public void createInputTopics() {
        var topics = new ArrayList<KeyValue<String, TsAdmin.CleanupConfig>>();
        topics.add(KeyValue.pair(inputTopicNames.getProjectEdges(), TsAdmin.CleanupConfig.COMPACT));
        topics.add(KeyValue.pair(inputTopicNames.proEntityLabelConfig(), TsAdmin.CleanupConfig.COMPACT));
        createTopics(topics);
    }

    /**
     * Creates the output topics with the desired configuration.
     * Use this in test before running the topology.
     */
    public void createOutputTopics() {
        var topics = new ArrayList<KeyValue<String, TsAdmin.CleanupConfig>>();
        topics.add(KeyValue.pair(outputTopicNames.labelConfigByProjectClass(), TsAdmin.CleanupConfig.COMPACT));
        topics.add(KeyValue.pair(outputTopicNames.entityLabels(), TsAdmin.CleanupConfig.COMPACT));
        topics.add(KeyValue.pair(outputTopicNames.labelEdgeBySource(), TsAdmin.CleanupConfig.COMPACT));
        topics.add(KeyValue.pair(outputTopicNames.labelEdgeByTarget(), TsAdmin.CleanupConfig.COMPACT));
        topics.add(KeyValue.pair(outputTopicNames.labelEdgesToolboxCommunityBySource(), TsAdmin.CleanupConfig.COMPACT));
        topics.add(KeyValue.pair(outputTopicNames.entityLabelsToolboxCommunity(), TsAdmin.CleanupConfig.COMPACT));
        topics.add(KeyValue.pair(outputTopicNames.entityLabelsToolboxProject(), TsAdmin.CleanupConfig.COMPACT));
        topics.add(KeyValue.pair(outputTopicNames.entityLabelsPublicCommunity(), TsAdmin.CleanupConfig.COMPACT));
        topics.add(KeyValue.pair(outputTopicNames.entityLanguageLabelsToolboxCommunity(), TsAdmin.CleanupConfig.COMPACT));
        topics.add(KeyValue.pair(outputTopicNames.entityLanguageLabelsPublicCommunity(), TsAdmin.CleanupConfig.COMPACT));

        createTopics(topics);
    }

    private void createTopics(List<KeyValue<String, TsAdmin.CleanupConfig>> topics) {
        var admin = new TsAdmin(bootstrapServers);
        // create output topics (with number of partitions and delete.policy=compact)
        admin.createOrConfigureTopics(topics, outputTopicPartitions, outputTopicReplicationFactor);
    }

}
