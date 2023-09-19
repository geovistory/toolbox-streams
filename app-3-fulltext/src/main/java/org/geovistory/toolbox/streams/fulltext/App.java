package org.geovistory.toolbox.streams.fulltext;/*
 * This Java source file was generated by the Gradle 'init' task.
 */

import org.apache.kafka.streams.Topology;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.fulltext.processors.community.CommunityEntityFulltext;
import org.geovistory.toolbox.streams.fulltext.processors.project.ProjectEntityFulltext;
import org.geovistory.toolbox.streams.lib.TsAdmin;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import java.util.ArrayList;


@ApplicationScoped
public class App {

    @ConfigProperty(name = "ts.output.topic.partitions")
    int outputTopicPartitions;
    @ConfigProperty(name = "ts.output.topic.replication.factor")
    short outputTopicReplicationFactor;

    @ConfigProperty(name = "quarkus.kafka.streams.bootstrap.servers")
    String bootstrapServers;
    @Inject
    ProjectEntityFulltext projectEntityFulltext;
    @Inject
    CommunityEntityFulltext communityEntityFulltext;
    @Inject
    BuilderSingleton builderSingleton;
    @Inject
    RegisterInputTopic registerInputTopic;
    @Inject
    OutputTopicNames outputTopicNames;

    Boolean initialized = false;

    //  All we need to do for that is to declare a CDI producer method which returns the Kafka Streams Topology; the Quarkus extension will take care of configuring, starting and stopping the actual Kafka Streams engine.
    @Produces
    public Topology buildTopology() {

        // add processors of sub-topologies
        if (!initialized) {
            initialized = true;
            addSubTopologies();
        }

        // create topics in advance to ensure correct configuration (partition, compaction, ect.)
        createTopics();

        // build the topology
        return builderSingleton.builder.build();
    }


    private void addSubTopologies() {

        addProjectView();

        addCommunityToolboxView();

    }

    private void createTopics() {
        var admin = new TsAdmin(bootstrapServers);
        createTopicsForProjectView(admin);
        createTopicsForCommunityView(admin);
    }

    private void addProjectView() {
        // register input topics as KTables
        var projectEntityWithLabelConfigTable = registerInputTopic.projectEntityWithLabelConfigTable();
        var projectTopStatementsTable = registerInputTopic.projectTopStatementsTable();
        var projectPropertyLabelTable = registerInputTopic.projectPropertyLabelTable();


        // add sub-topology ProjectEntityFulltext
        projectEntityFulltext.addProcessors(
                projectEntityWithLabelConfigTable,
                projectTopStatementsTable,
                projectPropertyLabelTable
        );

    }

    private void addCommunityToolboxView() {
        // register input topics as KTables
        var communityEntityWithLabelConfigTable = registerInputTopic.communityEntityWithLabelConfigTable();
        var communityTopStatementsTable = registerInputTopic.communityTopStatementsTable();
        var communityPropertyLabelTable = registerInputTopic.communityPropertyLabelTable();


        // add sub-topology CommunityEntityFulltext
        communityEntityFulltext.addProcessors(
                communityEntityWithLabelConfigTable,
                communityTopStatementsTable,
                communityPropertyLabelTable
        );

    }


    private void createTopicsForProjectView(TsAdmin admin) {
        // create output topics (with number of partitions and delete.policy=compact)
        var topics = new ArrayList<String>();
        topics.add(outputTopicNames.projectEntityFulltext());
        admin.createOrConfigureTopics(topics, outputTopicPartitions, outputTopicReplicationFactor);
    }


    private void createTopicsForCommunityView(TsAdmin admin) {
        // create output topics (with number of partitions and delete.policy=compact)
        var topics = new ArrayList<String>();
        topics.add(outputTopicNames.communityEntityFulltext());
        admin.createOrConfigureTopics(topics, outputTopicPartitions, outputTopicReplicationFactor);
    }
}
