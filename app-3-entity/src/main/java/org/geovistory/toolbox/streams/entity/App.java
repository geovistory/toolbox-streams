/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package org.geovistory.toolbox.streams.entity;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.geovistory.toolbox.streams.avro.HasTypePropertyKey;
import org.geovistory.toolbox.streams.avro.HasTypePropertyValue;
import org.geovistory.toolbox.streams.avro.OntomeClassKey;
import org.geovistory.toolbox.streams.avro.OntomeClassMetadataValue;
import org.geovistory.toolbox.streams.entity.processors.community.CommunityEntityClassLabel;
import org.geovistory.toolbox.streams.entity.processors.community.CommunityEntityClassMetadata;
import org.geovistory.toolbox.streams.entity.processors.community.CommunityEntityTimeSpan;
import org.geovistory.toolbox.streams.entity.processors.community.CommunityEntityType;
import org.geovistory.toolbox.streams.entity.processors.project.ProjectEntityClassLabel;
import org.geovistory.toolbox.streams.entity.processors.project.ProjectEntityClassMetadata;
import org.geovistory.toolbox.streams.entity.processors.project.ProjectEntityTimeSpan;
import org.geovistory.toolbox.streams.entity.processors.project.ProjectEntityType;
import org.geovistory.toolbox.streams.lib.Admin;
import org.geovistory.toolbox.streams.lib.AppConfig;

import static org.geovistory.toolbox.streams.entity.BuildProperties.getDockerImageTag;
import static org.geovistory.toolbox.streams.entity.BuildProperties.getDockerTagSuffix;


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
        System.out.println("Starting Toolbox Streams App " + getDockerImageTag() + ":" + getDockerTagSuffix());
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
        var inputTopic = new RegisterInputTopic(builder);
        var hasTypePropertyTable = inputTopic.hasTypePropertyTable();
        var ontomeClassMetadataTable = inputTopic.ontomeClassMetadataTable();
        addProjectView(
                builder,
                inputTopic,
                hasTypePropertyTable,
                ontomeClassMetadataTable
        );
        addCommunityToolboxView(
                builder,
                inputTopic,
                "toolbox",
                hasTypePropertyTable,
                ontomeClassMetadataTable
        );

    }

    private static void createTopics() {
        var admin = new Admin();

        var outputTopicPartitions = Integer.parseInt(AppConfig.INSTANCE.getOutputTopicPartitions());
        var outputTopicReplicationFactor = Short.parseShort(AppConfig.INSTANCE.getOutputTopicReplicationFactor());

        createTopicsForProjectView(admin, outputTopicPartitions, outputTopicReplicationFactor);
        createTopicsForCommunityView(admin, outputTopicPartitions, outputTopicReplicationFactor, "toolbox");

    }

    private static void addProjectView(
            StreamsBuilder builder,
            RegisterInputTopic inputTopic,
            KTable<HasTypePropertyKey, HasTypePropertyValue> hasTypePropertyTable,
            KTable<OntomeClassKey, OntomeClassMetadataValue> ontomeClassMetadataTable
    ) {
        // register input topics as KTables
        var projectEntityTable = inputTopic.projectEntityTable();
        var projectTopOutgoingStatementsTable = inputTopic.projectTopOutgoingStatementsTable();

        // register input topics as KStreams
        var projectClassLabelTable = inputTopic.projectClassLabelTable();
        var projectTopOutgoingStatementsStream = inputTopic.projectTopOutgoingStatementsStream();

        // add sub-topology ProjectEntityTimeSpan
        ProjectEntityTimeSpan.addProcessors(builder,
                projectTopOutgoingStatementsStream
        );

        // add sub-topology ProjectEntityType
        ProjectEntityType.addProcessors(builder,
                projectEntityTable,
                hasTypePropertyTable,
                projectTopOutgoingStatementsTable
        );

        // add sub-topology ProjectEntityClassLabel
        ProjectEntityClassLabel.addProcessors(builder,
                projectEntityTable,
                projectClassLabelTable
        );

        // add sub-topology ProjectEntityClassMetadata
        ProjectEntityClassMetadata.addProcessors(builder,
                projectEntityTable,
                ontomeClassMetadataTable
        );
    }

    private static void addCommunityToolboxView(
            StreamsBuilder builder,
            RegisterInputTopic inputTopic,
            String nameSupplement,
            KTable<HasTypePropertyKey, HasTypePropertyValue> hasTypePropertyTable,
            KTable<OntomeClassKey, OntomeClassMetadataValue> ontomeClassMetadataTable
    ) {
        // register input topics as KTables
        var communityEntityTable = inputTopic.communityEntityTable();
        var communityTopOutgoingStatementsTable = inputTopic.communityTopOutgoingStatementsTable();


        // register input topics as KStreams
        var communityClassLabelTable = inputTopic.communityClassLabelTable();
        var communityTopOutgoingStatementsStream = inputTopic.communityTopOutgoingStatementsStream();


        // add sub-topology CommunityEntityTimeSpan
        CommunityEntityTimeSpan.addProcessors(builder,
                communityTopOutgoingStatementsStream,
                nameSupplement
        );

        // add sub-topology CommunityEntityType
        CommunityEntityType.addProcessors(builder,
                communityEntityTable,
                hasTypePropertyTable,
                communityTopOutgoingStatementsTable,
                nameSupplement
        );

        // add sub-topology CommunityEntityClassLabel
        CommunityEntityClassLabel.addProcessors(builder,
                communityEntityTable,
                communityClassLabelTable,
                nameSupplement
        );

        // add sub-topology CommunityEntityClassMetadata
        CommunityEntityClassMetadata.addProcessors(builder,
                communityEntityTable,
                ontomeClassMetadataTable,
                nameSupplement
        );
    }


    private static void createTopicsForProjectView(Admin admin, int outputTopicPartitions, short outputTopicReplicationFactor) {
        // create output topics (with number of partitions and delete.policy=compact)
        admin.createOrConfigureTopics(new String[]{
                ProjectEntityClassLabel.output.TOPICS.project_entity_class_label,
                ProjectEntityTimeSpan.output.TOPICS.project_entity_time_span,
                ProjectEntityClassMetadata.output.TOPICS.project_entity_class_metadata,
                ProjectEntityType.output.TOPICS.project_entity_type,
        }, outputTopicPartitions, outputTopicReplicationFactor);
    }


    private static void createTopicsForCommunityView(
            Admin admin,
            int outputTopicPartitions,
            short outputTopicReplicationFactor,
            String nameSupplement) {
        // create output topics (with number of partitions and delete.policy=compact)
        admin.createOrConfigureTopics(new String[]{
                CommunityEntityClassLabel.getOutputTopicName(nameSupplement),
                CommunityEntityTimeSpan.getOutputTopicName(nameSupplement),
                CommunityEntityClassMetadata.getOutputTopicName(nameSupplement),
                CommunityEntityType.getOutputTopicName(nameSupplement),
        }, outputTopicPartitions, outputTopicReplicationFactor);
    }


}
