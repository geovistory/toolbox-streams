/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package org.geovistory.toolbox.streams.entity.label;

import org.apache.kafka.streams.Topology;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.entity.label.processsors.base.ProjectEntityVisibility;
import org.geovistory.toolbox.streams.entity.label.processsors.community.*;
import org.geovistory.toolbox.streams.entity.label.processsors.project.*;
import org.geovistory.toolbox.streams.lib.TsAdmin;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import java.util.ArrayList;

@ApplicationScoped
class App {

    @ConfigProperty(name = "ts.output.topic.partitions")
    int outputTopicPartitions;
    @ConfigProperty(name = "ts.output.topic.replication.factor")
    short outputTopicReplicationFactor;

    @Inject
    ProjectStatementWithEntity projectStatementWithEntity;
    @Inject
    ProjectStatementWithLiteral projectStatementWithLiteral;
    @Inject
    ProjectTopIncomingStatements projectTopIncomingStatements;
    @Inject
    ProjectTopOutgoingStatements projectTopOutgoingStatements;
    @Inject
    ProjectTopStatements projectTopStatements;
    @Inject
    ProjectEntityVisibility projectEntityVisibility;
    @Inject
    ProjectEntity projectEntity;
    @Inject
    ProjectEntityLabel projectEntityLabel;
    @Inject
    CommunityToolboxStatementWithEntity communityToolboxStatementWithEntity;
    @Inject
    CommunityToolboxEntity communityToolboxEntity;
    @Inject
    CommunityToolboxStatementWithLiteral communityToolboxStatementWithLiteral;
    @Inject
    CommunityToolboxTopIncomingStatements communityToolboxTopIncomingStatements;
    @Inject
    CommunityToolboxTopOutgoingStatements communityToolboxTopOutgoingStatements;
    @Inject
    CommunityToolboxTopStatements communityToolboxTopStatements;
    @Inject
    CommunityToolboxEntityLabel communityToolboxEntityLabel;

    @ConfigProperty(name = "quarkus.kafka.streams.bootstrap.servers")
    String bootstrapServers;
    @Inject
    BuilderSingleton builderSingleton;
    @Inject
    RegisterInputTopic registerInputTopic;
    @Inject
    RegisterInnerTopic registerInnerTopic;

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

        // register input topics as KTables
        var proInfoProjRelTable = registerInputTopic.proInfoProjRelTable();
        var infResourceTable = registerInputTopic.infResourceTable();
        var statementWithEntityTable = registerInputTopic.statementWithEntityTable();
        var statementWithLiteralTable = registerInputTopic.statementWithLiteralTable();
        var projectEntityLabelConfigTable = registerInputTopic.projectEntityLabelConfigTable();
        var communityEntityLabelConfigTable = registerInputTopic.communityEntityLabelConfigTable();

        // register inner topics as KTables
        var projectEntityLabelTable = registerInnerTopic.projectEntityLabelTable();
        var projectEntityTable = registerInnerTopic.projectEntityTable();
        var projectStatementWithEntityTable = registerInnerTopic.projectStatementWithEntityTable();
        var communityToolboxEntityLabelTable = registerInnerTopic.communityToolboxEntityLabelTable();
        var communityToolboxEntityTable = registerInnerTopic.communityToolboxEntityTable();
        var communityToolboxStatementWithEntityTable = registerInnerTopic.communityToolboxStatementWithEntityTable();

        // add sub-topology ProjectStatementWithEntity
        var projectStatementWithEntityReturn = projectStatementWithEntity.addProcessors(
                statementWithEntityTable,
                proInfoProjRelTable
        );

        // add sub-topology ProjectStatementWithLiteral
        var projectStatementWithLiteralReturn = projectStatementWithLiteral.addProcessors(
                statementWithLiteralTable,
                proInfoProjRelTable
        );
        // add sub-topology ProjectTopIncomingStatements
        var projectTopIncomingStatementsReturn = projectTopIncomingStatements.addProcessors(
                projectStatementWithEntityTable,
                projectEntityLabelTable,
                communityToolboxEntityLabelTable
        );

        // add sub-topology ProjectTopOutgoingStatements
        var projectTopOutgoingStatementsReturn = projectTopOutgoingStatements.addProcessors(
                projectStatementWithLiteralReturn.ProjectStatementStream(),
                projectStatementWithEntityTable,
                projectEntityLabelTable,
                communityToolboxEntityLabelTable
        );

        // add sub-topology ProjectTopStatements
        var projectTopStatementsReturn = projectTopStatements.addProcessors(
                projectTopOutgoingStatementsReturn.projectTopStatementStream(),
                projectTopIncomingStatementsReturn.projectTopStatementStream()
        );


        // add sub-topology ProjectEntityVisibility
        var projectEntityVisibilityReturn = projectEntityVisibility.addProcessors(
                infResourceTable,
                proInfoProjRelTable
        );

        // add sub-topology ProjectEntity
        projectEntity.addProcessors(
                projectEntityVisibilityReturn.projectEntityVisibilityStream()
        );


        // add sub-topology ProjectEntityLabel
        projectEntityLabel.addProcessors(
                projectEntityTable,
                projectEntityLabelConfigTable,
                projectTopStatementsReturn.projectTopStatementTable()
        );


        // add sub-topology CommunityToolboxStatementWithEntity
        communityToolboxStatementWithEntity.addProcessors(
                projectStatementWithEntityReturn.ProjectStatementStream()
        );

        // add sub-topology CommunityToolboxEntity
        communityToolboxEntity.addProcessors(
                projectEntityVisibilityReturn.projectEntityVisibilityStream()
        );

        // add sub-topology CommunityToolboxStatementWithLiteral
        var communityToolboxStatementWithLiteralReturn = communityToolboxStatementWithLiteral.addProcessors(
                projectStatementWithLiteralReturn.ProjectStatementStream()
        );
        // add sub-topology CommunityToolboxTopIncomingStatements
        var communityToolboxTopIncomingStatementsReturn = communityToolboxTopIncomingStatements.addProcessors(
                communityToolboxStatementWithEntityTable,
                communityToolboxEntityLabelTable
        );
        // add sub-topology CommunityToolboxTopOutgoingStatements
        var communityToolboxTopOutgoingStatementsReturn = communityToolboxTopOutgoingStatements.addProcessors(
                communityToolboxStatementWithLiteralReturn.communityStatementStream(),
                communityToolboxStatementWithEntityTable,
                communityToolboxEntityLabelTable
        );

        // add sub-topology CommunityToolboxTopStatements
        var communityToolboxTopStatementsReturn = communityToolboxTopStatements.addProcessors(
                communityToolboxTopIncomingStatementsReturn.communityTopStatementStream(),
                communityToolboxTopOutgoingStatementsReturn.communityTopStatementStream()
        );

        // add sub-topology CommunityToolboxEntityLabel
        communityToolboxEntityLabel.addProcessors(
                communityToolboxEntityTable,
                communityEntityLabelConfigTable,
                communityToolboxTopStatementsReturn.communityTopStatementTable()
        );


    }

    private void createTopics() {
        var admin = new TsAdmin(bootstrapServers);
        // create output topics (with number of partitions and delete.policy=compact)
        var topics = new ArrayList<String>();
        topics.add(outputTopicNames.projectEntityVisibility());
        topics.add(outputTopicNames.projectEntity());
        topics.add(outputTopicNames.projectStatementWithEntity());
        topics.add(outputTopicNames.projectStatementWithLiteral());
        topics.add(outputTopicNames.projectTopOutgoingStatements());
        topics.add(outputTopicNames.projectTopIncomingStatements());
        topics.add(outputTopicNames.projectTopStatements());
        topics.add(outputTopicNames.projectEntityLabel());
        topics.add(outputTopicNames.projectEntityWithLabelConfig());
        topics.add(outputTopicNames.communityToolboxEntity());
        topics.add(outputTopicNames.communityToolboxStatementWithEntity());
        topics.add(outputTopicNames.communityToolboxStatementWithLiteral());
        topics.add(outputTopicNames.communityToolboxTopIncomingStatements());
        topics.add(outputTopicNames.communityToolboxTopOutgoingStatements());
        topics.add(outputTopicNames.communityToolboxTopStatements());
        topics.add(outputTopicNames.communityToolboxEntityLabel());
        topics.add(outputTopicNames.communityToolboxEntityWithLabelConfig());

        admin.createOrConfigureTopics(topics, outputTopicPartitions, outputTopicReplicationFactor);
    }


}
