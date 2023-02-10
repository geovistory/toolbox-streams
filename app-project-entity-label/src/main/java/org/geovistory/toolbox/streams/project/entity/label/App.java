/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package org.geovistory.toolbox.streams.project.entity.label;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.geovistory.toolbox.streams.lib.Admin;
import org.geovistory.toolbox.streams.lib.AppConfig;
import org.geovistory.toolbox.streams.project.entity.label.processors.*;

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
        System.out.println("Starting Toolbox Streams App v" + BuildProperties.getDockerTagSuffix());
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

        // register input topics as KTables
        var inputTopics = new RegisterInputTopics(builder);
        var proInfoProjRelTable = inputTopics.proInfoProjRelTable();
        var infResourceTable = inputTopics.infResourceTable();
        var statementWithEntityTable = inputTopics.statementWithEntityTable();
        var statementWithLiteralTable = inputTopics.statementWithLiteralTable();
        var projectEntityLabelConfigTable = inputTopics.projectEntityLabelConfigTable();

        // register inner topics as KTables
        var innerTopics = new RegisterInnerTopic(builder);
        var projectEntityLabelTable = innerTopics.projectEntityLabelTable();
        var projectEntityTable = innerTopics.projectEntityTable();
        var projectStatementWithEntityTable = innerTopics.projectStatementWithEntityTable();

        // add sub-topology ProjectStatementWithEntity
        ProjectStatementWithEntity.addProcessors(builder,
                statementWithEntityTable,
                proInfoProjRelTable
        );

        // add sub-topology ProjectStatementWithLiteral
        var projectStatementWithLiteral = ProjectStatementWithLiteral.addProcessors(builder,
                statementWithLiteralTable,
                proInfoProjRelTable
        );
        // add sub-topology ProjectTopIncomingStatements
        var projectTopIncomingStatements = ProjectTopIncomingStatements.addProcessors(builder,
                projectStatementWithEntityTable,
                projectEntityLabelTable
        );

        // add sub-topology ProjectTopOutgoingStatements
        var projectTopOutgoingStatements = ProjectTopOutgoingStatements.addProcessors(builder,
                projectStatementWithLiteral.ProjectStatementStream(),
                projectStatementWithEntityTable,
                projectEntityLabelTable
        );

        // add sub-topology ProjectTopStatements
        var projectTopStatements = ProjectTopStatements.addProcessors(builder,
                projectTopOutgoingStatements.projectTopStatementStream(),
                projectTopIncomingStatements.projectTopStatementStream()
        );

        // add sub-topology ProjectEntity
        ProjectEntity.addProcessors(builder,
                infResourceTable,
                proInfoProjRelTable
        );


        // add sub-topology ProjectEntityLabel
        ProjectEntityLabel.addProcessors(builder,
                projectEntityTable,
                projectEntityLabelConfigTable,
                projectTopStatements.projectTopStatementTable()
        );



    }

    private static void createTopics() {
        var admin = new Admin();

        var outputTopicPartitions = Integer.parseInt(AppConfig.INSTANCE.getOutputTopicPartitions());
        var outputTopicReplicationFactor = Short.parseShort(AppConfig.INSTANCE.getOutputTopicReplicationFactor());

        // create output topics (with number of partitions and delete.policy=compact)
        admin.createOrConfigureTopics(new String[]{
                ProjectEntity.output.TOPICS.project_entity,
                ProjectStatementWithEntity.output.TOPICS.project_statement_with_entity,
                ProjectTopOutgoingStatements.output.TOPICS.project_top_outgoing_statements,
                ProjectTopIncomingStatements.output.TOPICS.project_top_incoming_statements,
                ProjectTopStatements.output.TOPICS.project_top_statements,
                ProjectEntityLabel.output.TOPICS.project_entity_label,
                ProjectEntityTopStatements.output.TOPICS.project_entity_top_statements,
        }, outputTopicPartitions, outputTopicReplicationFactor);
    }


}
