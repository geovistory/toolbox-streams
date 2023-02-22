/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package org.geovistory.toolbox.streams.entity.label;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.geovistory.toolbox.streams.entity.label.processsors.base.ProjectEntityVisibility;
import org.geovistory.toolbox.streams.entity.label.processsors.community.*;
import org.geovistory.toolbox.streams.entity.label.processsors.project.*;
import org.geovistory.toolbox.streams.lib.Admin;
import org.geovistory.toolbox.streams.lib.AppConfig;

import static org.geovistory.toolbox.streams.entity.label.BuildProperties.getDockerImageTag;
import static org.geovistory.toolbox.streams.entity.label.BuildProperties.getDockerTagSuffix;

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

        // register input topics as KTables
        var inputTopics = new RegisterInputTopics(builder);
        var proInfoProjRelTable = inputTopics.proInfoProjRelTable();
        var infResourceTable = inputTopics.infResourceTable();
        var statementWithEntityTable = inputTopics.statementWithEntityTable();
        var statementWithLiteralTable = inputTopics.statementWithLiteralTable();
        var projectEntityLabelConfigTable = inputTopics.projectEntityLabelConfigTable();
        var communityEntityLabelConfigTable = inputTopics.communityEntityLabelConfigTable();

        // register inner topics as KTables
        var innerTopics = new RegisterInnerTopic(builder);
        var projectEntityLabelTable = innerTopics.projectEntityLabelTable();
        var projectEntityTable = innerTopics.projectEntityTable();
        var projectStatementWithEntityTable = innerTopics.projectStatementWithEntityTable();
        var communityToolboxEntityLabelTable = innerTopics.communityToolboxEntityLabelTable();
        var communityToolboxEntityTable = innerTopics.communityToolboxEntityTable();
        var communityToolboxStatementWithEntityTable = innerTopics.communityToolboxStatementWithEntityTable();

        // add sub-topology ProjectStatementWithEntity
        var projectStatementWithEntity = ProjectStatementWithEntity.addProcessors(builder,
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



        // add sub-topology ProjectEntityVisibility
        var projectEntityVisibility = ProjectEntityVisibility.addProcessors(builder,
                infResourceTable,
                proInfoProjRelTable
        );

        // add sub-topology ProjectEntity
        ProjectEntity.addProcessors(builder,
                projectEntityVisibility.projectEntityVisibilityStream()
        );


        // add sub-topology ProjectEntityLabel
        ProjectEntityLabel.addProcessors(builder,
                projectEntityTable,
                projectEntityLabelConfigTable,
                projectTopStatements.projectTopStatementTable()
        );


        // add sub-topology CommunityToolboxStatementWithEntity
        CommunityToolboxStatementWithEntity.addProcessors(builder,
                projectStatementWithEntity.ProjectStatementStream()
        );

        // add sub-topology CommunityToolboxStatementWithLiteral
        var communityToolboxStatementWithLiteral = CommunityToolboxStatementWithLiteral.addProcessors(builder,
                projectStatementWithLiteral.ProjectStatementStream()
        );
        // add sub-topology CommunityToolboxTopIncomingStatements
        var communityToolboxTopIncomingStatements = CommunityToolboxTopIncomingStatements.addProcessors(builder,
                communityToolboxStatementWithEntityTable,
                communityToolboxEntityLabelTable
        );
        // add sub-topology CommunityToolboxTopOutgoingStatements
        var communityToolboxTopOutgoingStatements = CommunityToolboxTopOutgoingStatements.addProcessors(builder,
                communityToolboxStatementWithLiteral.communityStatementStream(),
                communityToolboxStatementWithEntityTable,
                communityToolboxEntityLabelTable
        );

        // add sub-topology CommunityToolboxTopStatements
        var communityToolboxTopStatements = CommunityToolboxTopStatements.addProcessors(builder,
                communityToolboxTopIncomingStatements.communityTopStatementStream(),
                communityToolboxTopOutgoingStatements.communityTopStatementStream()
        );

        // add sub-topology CommunityToolboxEntityLabel
        CommunityToolboxEntityLabel.addProcessors(builder,
                communityToolboxEntityTable,
                communityEntityLabelConfigTable,
                communityToolboxTopStatements.communityTopStatementTable()
        );



    }

    private static void createTopics() {
        var admin = new Admin();

        var outputTopicPartitions = Integer.parseInt(AppConfig.INSTANCE.getOutputTopicPartitions());
        var outputTopicReplicationFactor = Short.parseShort(AppConfig.INSTANCE.getOutputTopicReplicationFactor());

        // create output topics (with number of partitions and delete.policy=compact)
        admin.createOrConfigureTopics(new String[]{
                ProjectEntityVisibility.output.TOPICS.project_entity_visibility,

                ProjectEntity.output.TOPICS.project_entity,
                ProjectStatementWithEntity.output.TOPICS.project_statement_with_entity,
                ProjectStatementWithLiteral.output.TOPICS.project_statement_with_literal,
                ProjectTopOutgoingStatements.output.TOPICS.project_top_outgoing_statements,
                ProjectTopIncomingStatements.output.TOPICS.project_top_incoming_statements,
                ProjectTopStatements.output.TOPICS.project_top_statements,
                ProjectEntityLabel.output.TOPICS.project_entity_label,

                CommunityToolboxEntity.output.TOPICS.community_toolbox_entity,
                CommunityToolboxStatementWithEntity.output.TOPICS.community_toolbox_statement_with_entity,
                CommunityToolboxStatementWithLiteral.output.TOPICS.community_toolbox_statement_with_literal,
                CommunityToolboxTopIncomingStatements.output.TOPICS.community_toolbox_top_incoming_statements,
                CommunityToolboxTopOutgoingStatements.output.TOPICS.community_toolbox_top_outgoing_statements,
                CommunityToolboxTopStatements.output.TOPICS.community_toolbox_top_statements,
                CommunityToolboxEntityLabel.output.TOPICS.community_toolbox_entity_label
        }, outputTopicPartitions, outputTopicReplicationFactor);
    }


}
