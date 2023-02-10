/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package org.geovistory.toolbox.streams.project.entity;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.geovistory.toolbox.streams.lib.Admin;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.project.entity.topologies.*;

import static org.geovistory.toolbox.streams.project.entity.BuildProperties.getDockerImageTag;
import static org.geovistory.toolbox.streams.project.entity.BuildProperties.getDockerTagSuffix;

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

        // register input topics as KTables
        var projectEntityLabelConfigTable = inputTopic.projectEntityLabelConfigTable();
        var projectEntityTable = inputTopic.projectEntityTable();
        var projectTopOutgoingStatementsTable = inputTopic.projectTopOutgoingStatementsTable();
        var hasTypePropertyTable = inputTopic.hasTypePropertyTable();
        var ontomeClassMetadataTable = inputTopic.ontomeClassMetadataTable();

        // register input topics as KStreams
        var projectClassLabelTable = inputTopic.projectClassLabelTable();
        var projectTopStatementsTable = inputTopic.projectTopStatementsTable();
        var projectPropertyLabelTable = inputTopic.projectPropertyLabelTable();

        // add sub-topology ProjectEntityTopStatements
        var projectEntityTopStatements = ProjectEntityTopStatements.addProcessors(builder,
                projectEntityTable,
                projectTopStatementsTable,
                projectPropertyLabelTable
        );

        // add sub-topology ProjectEntityFulltext
        ProjectEntityFulltext.addProcessors(builder,
                projectEntityTopStatements.projectEntityTopStatementTable(),
                projectEntityLabelConfigTable
        );

        // add sub-topology ProjectEntityTimeSpan
        ProjectEntityTimeSpan.addProcessors(builder,
                projectEntityTopStatements.projectEntityTopStatementStream()
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

    private static void createTopics() {
        var admin = new Admin();

        var outputTopicPartitions = Integer.parseInt(AppConfig.INSTANCE.getOutputTopicPartitions());
        var outputTopicReplicationFactor = Short.parseShort(AppConfig.INSTANCE.getOutputTopicReplicationFactor());

        // create output topics (with number of partitions and delete.policy=compact)
        admin.createOrConfigureTopics(new String[]{
                ProjectEntityClassLabel.output.TOPICS.project_entity_class_label,
                ProjectEntityFulltext.output.TOPICS.project_entity_fulltext,
                ProjectEntityTimeSpan.output.TOPICS.project_entity_time_span,
                ProjectEntityClassMetadata.output.TOPICS.project_entity_class_metadata,
                ProjectEntityType.output.TOPICS.project_entity_type,
                ProjectEntityTopStatements.output.TOPICS.project_entity_top_statements,
        }, outputTopicPartitions, outputTopicReplicationFactor);


    }


}
