package org.geovistory.toolbox.streams.fulltext;/*
 * This Java source file was generated by the Gradle 'init' task.
 */

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.geovistory.toolbox.streams.fulltext.processors.community.CommunityEntityFulltext;
import org.geovistory.toolbox.streams.fulltext.processors.project.ProjectEntityFulltext;
import org.geovistory.toolbox.streams.lib.Admin;
import org.geovistory.toolbox.streams.lib.AppConfig;

import java.util.ArrayList;

import static org.geovistory.toolbox.streams.fulltext.BuildProperties.getDockerImageTag;
import static org.geovistory.toolbox.streams.fulltext.BuildProperties.getDockerTagSuffix;


class App {
    public static void main(String[] args) {


        StreamsBuilder builder = new StreamsBuilder();

        // add processors of sub-topologies
        addSubTopologies(builder);

        // build the topology
        var topology = builder.build();

        System.out.println(topology.describe());

        // create topics in advance to ensure correct configuration (partition, compaction, ect.)
        createTopics();

        // print configuration information
        System.out.println("Starting Toolbox Streams Fulltext org.geovistory.toolbox.streams.fulltext.App " + getDockerImageTag() + ":" + getDockerTagSuffix());
        System.out.println("With config:");
        AppConfig.INSTANCE.printConfigs();

        // create the streams app
        // noinspection resource
        KafkaStreams streams = new KafkaStreams(topology, AppConfig.getConfig());

        // close Kafka Streams when the JVM shuts down (e.g. SIGTERM)
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        // start streaming!
        streams.start();
    }


    private static void addSubTopologies(StreamsBuilder builder) {
        var inputTopic = new org.geovistory.toolbox.streams.fulltext.RegisterInputTopic(builder);

        addProjectView(inputTopic);

        addCommunityToolboxView(inputTopic, "toolbox");

    }

    private static void createTopics() {
        var admin = new Admin();

        var outputTopicPartitions = Integer.parseInt(AppConfig.INSTANCE.getOutputTopicPartitions());
        var outputTopicReplicationFactor = Short.parseShort(AppConfig.INSTANCE.getOutputTopicReplicationFactor());

        createTopicsForProjectView(admin, outputTopicPartitions, outputTopicReplicationFactor);
        createTopicsForCommunityView(admin, outputTopicPartitions, outputTopicReplicationFactor, "toolbox");

    }

    private static void addProjectView(
            org.geovistory.toolbox.streams.fulltext.RegisterInputTopic inputTopic) {
        // register input topics as KTables
        var projectEntityWithLabelConfigTable = inputTopic.projectEntityWithLabelConfigTable();
        var projectTopStatementsTable = inputTopic.projectTopStatementsTable();
        var projectPropertyLabelTable = inputTopic.projectPropertyLabelTable();


        // add sub-topology ProjectEntityFulltext
        ProjectEntityFulltext.addProcessors(
                projectEntityWithLabelConfigTable,
                projectTopStatementsTable,
                projectPropertyLabelTable
        );

    }

    private static void addCommunityToolboxView(
            org.geovistory.toolbox.streams.fulltext.RegisterInputTopic inputTopic,
            String nameSupplement
    ) {
        // register input topics as KTables
        var communityEntityWithLabelConfigTable = inputTopic.communityEntityWithLabelConfigTable();
        var communityTopStatementsTable = inputTopic.communityTopStatementsTable();
        var communityPropertyLabelTable = inputTopic.communityPropertyLabelTable();


        // add sub-topology CommunityEntityFulltext
        CommunityEntityFulltext.addProcessors(
                communityEntityWithLabelConfigTable,
                communityTopStatementsTable,
                communityPropertyLabelTable,
                nameSupplement
        );

    }


    private static void createTopicsForProjectView(Admin admin, int outputTopicPartitions, short outputTopicReplicationFactor) {
        // create output topics (with number of partitions and delete.policy=compact)
        var topics = new ArrayList<String>();
        topics.add(ProjectEntityFulltext.output.TOPICS.project_entity_fulltext);
        admin.createOrConfigureTopics(topics, outputTopicPartitions, outputTopicReplicationFactor);
    }


    private static void createTopicsForCommunityView(
            Admin admin,
            int outputTopicPartitions,
            short outputTopicReplicationFactor,
            String nameSupplement) {
        // create output topics (with number of partitions and delete.policy=compact)
        var topics = new ArrayList<String>();
        topics.add(CommunityEntityFulltext.getOutputTopicName(nameSupplement));

        admin.createOrConfigureTopics(topics, outputTopicPartitions, outputTopicReplicationFactor);
    }


}
