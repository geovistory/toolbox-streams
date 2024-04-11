/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package org.geovistory.toolbox.streams.entity.preview;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.entity.preview.processors.EntityPreview;
import org.geovistory.toolbox.streams.entity.preview.processors.community.CommunityEntityPreview;
import org.geovistory.toolbox.streams.entity.preview.processors.project.ProjectEntityPreview;
import org.geovistory.toolbox.streams.lib.TsAdmin;

import java.util.ArrayList;
import java.util.Objects;


@ApplicationScoped
public class App {

    @ConfigProperty(name = "ts.output.topic.partitions")
    int outputTopicPartitions;
    @ConfigProperty(name = "ts.output.topic.replication.factor")
    short outputTopicReplicationFactor;

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrapServers;
    @Inject
    ProjectEntityPreview projectEntityPreview;
    @Inject
    CommunityEntityPreview communityEntityPreview;
    @Inject
    EntityPreview entityPreview;
    @Inject
    BuilderSingleton builderSingleton;
    @Inject
    RegisterInnerTopic registerInnerTopic;
    @Inject
    OutputTopicNames outputTopicNames;
    @ConfigProperty(name = "auto.create.output.topics")
    String autoCreateOutputTopics;


    //  All we need to do for that is to declare a CDI producer method which returns the Kafka Streams Topology; the Quarkus extension will take care of configuring, starting and stopping the actual Kafka Streams engine.
    @Produces
    public Topology buildTopology() {

        builderSingleton.builder = new StreamsBuilder();

        var topology = new Topology();

        // add processors of sub-topologies
        topology = addSubTopologies();

        // create output topics in advance to ensure correct configuration (partition, compaction, ect.)
        if (Objects.equals(autoCreateOutputTopics, "enabled")) createTopics();

        // build the topology
        return topology;
    }

    private Topology addSubTopologies() {
        addMergedView();
        var topology = builderSingleton.builder.build();
        communityEntityPreview.addProcessors(topology);
        projectEntityPreview.addProcessors(topology);
        return topology;
    }


    private void addMergedView() {
        // register inner topics as KStream
        var projectEntityPreviewStream = registerInnerTopic.projectEntityPreviewStream();
        var communityEntityPreviewStream = registerInnerTopic.communityEntityPreviewStream();

        // add sub-topology ProjectEntityPreview
        entityPreview.addProcessors(
                projectEntityPreviewStream,
                communityEntityPreviewStream
        );
    }

    private void createTopics() {
        var admin = new TsAdmin(bootstrapServers);

        // create output topics (with number of partitions and delete.policy=compact)
        var topics = new ArrayList<String>();
        topics.add(outputTopicNames.projectEntityPreview());
        topics.add(outputTopicNames.communityEntityPreview());
        topics.add(outputTopicNames.entityPreview());
        admin.createOrConfigureTopics(topics, outputTopicPartitions, outputTopicReplicationFactor);

    }

}
