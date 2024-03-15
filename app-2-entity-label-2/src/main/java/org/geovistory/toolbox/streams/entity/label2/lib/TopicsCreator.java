package org.geovistory.toolbox.streams.entity.label2.lib;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.streams.KeyValue;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.entity.label2.names.InputTopicNames;
import org.geovistory.toolbox.streams.entity.label2.names.OutputTopicNames;

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
        topics.add(KeyValue.pair(inputTopicNames.proInfProjRel(), TsAdmin.CleanupConfig.COMPACT));
        topics.add(KeyValue.pair(inputTopicNames.infResource(), TsAdmin.CleanupConfig.COMPACT));
        topics.add(KeyValue.pair(inputTopicNames.getStatementWithLiteral(), TsAdmin.CleanupConfig.COMPACT));
        topics.add(KeyValue.pair(inputTopicNames.getStatementWithEntity(), TsAdmin.CleanupConfig.COMPACT));
        createTopics(topics);
    }

    /**
     * Creates the output topics with the desired configuration.
     * Use this in test before running the topology.
     */
    public void createOutputTopics() {
        var topics = new ArrayList<KeyValue<String, TsAdmin.CleanupConfig>>();
        topics.add(KeyValue.pair(outputTopicNames.projectEntity(), TsAdmin.CleanupConfig.COMPACT));
        topics.add(KeyValue.pair(outputTopicNames.iprRepartitioned(), TsAdmin.CleanupConfig.COMPACT));
        topics.add(KeyValue.pair(outputTopicNames.eRepartitioned(), TsAdmin.CleanupConfig.COMPACT));
        topics.add(KeyValue.pair(outputTopicNames.sRepartitioned(), TsAdmin.CleanupConfig.COMPACT));
        topics.add(KeyValue.pair(outputTopicNames.projectStatementBySub(), TsAdmin.CleanupConfig.DELETE));
        topics.add(KeyValue.pair(outputTopicNames.projectStatementByOb(), TsAdmin.CleanupConfig.DELETE));
        topics.add(KeyValue.pair(outputTopicNames.projectEdges(), TsAdmin.CleanupConfig.COMPACT));
        topics.add(KeyValue.pair(outputTopicNames.projectStatementWithSubByPk(), TsAdmin.CleanupConfig.COMPACT));
        topics.add(KeyValue.pair(outputTopicNames.projectStatementWithObByPk(), TsAdmin.CleanupConfig.COMPACT));
        createTopics(topics);
    }

    private void createTopics(List<KeyValue<String, TsAdmin.CleanupConfig>> topics) {
        var admin = new TsAdmin(bootstrapServers);
        // create output topics (with number of partitions and delete.policy=compact)
        admin.createOrConfigureTopics(topics, outputTopicPartitions, outputTopicReplicationFactor);
    }

}
