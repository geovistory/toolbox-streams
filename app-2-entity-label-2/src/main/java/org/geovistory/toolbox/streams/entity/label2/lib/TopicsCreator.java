package org.geovistory.toolbox.streams.entity.label2.lib;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.entity.label2.names.InputTopicNames;
import org.geovistory.toolbox.streams.entity.label2.names.OutputTopicNames;
import org.geovistory.toolbox.streams.lib.TsAdmin;

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
        var topics = new ArrayList<String>();
        topics.add(inputTopicNames.proInfProjRel());
        topics.add(inputTopicNames.infResource());
        topics.add(inputTopicNames.getStatementWithLiteral());
        createTopics(topics);
    }

    /**
     * Creates the output topics with the desired configuration.
     * Use this in test before running the topology.
     */
    public void createOutputTopics() {
        var topics = new ArrayList<String>();
        topics.add(outputTopicNames.projectEntity());
        topics.add(outputTopicNames.projectStatement());
        topics.add(outputTopicNames.iprRepartitioned());
        topics.add(outputTopicNames.eRepartitioned());
        topics.add(outputTopicNames.sRepartitioned());
        createTopics(topics);
    }

    private void createTopics(List<String> topics) {
        var admin = new TsAdmin(bootstrapServers);
        // create output topics (with number of partitions and delete.policy=compact)
        admin.createOrConfigureTopics(topics, outputTopicPartitions, outputTopicReplicationFactor);
    }

}
