
package org.geovistory.toolbox.streams.entity.label;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.TsRegisterInputTopic;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * This class provides helper methods to register
 * output topics (generated by this app).
 * These helper methods are mainly used for testing.
 */
@ApplicationScoped
public class RegisterInnerTopic extends TsRegisterInputTopic {

    @Inject
    AvroSerdes avroSerdes;
    @Inject
    public BuilderSingleton builderSingleton;
    @Inject
    OutputTopicNames outputTopicNames;

    public RegisterInnerTopic(AvroSerdes avroSerdes, BuilderSingleton builderSingleton, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.builderSingleton = builderSingleton;
        this.outputTopicNames = outputTopicNames;
    }


    public KTable<ProjectEntityKey, ProjectEntityValue> projectEntityTable() {
        return getTable(builderSingleton.builder,
                outputTopicNames.projectEntity(),
                avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityValue());
    }


    public KTable<ProjectStatementKey, ProjectStatementValue> projectStatementWithEntityTable() {
        return getTable(builderSingleton.builder,
                outputTopicNames.projectStatementWithEntity(),
                avroSerdes.ProjectStatementKey(), avroSerdes.ProjectStatementValue());
    }

    public KTable<ProjectEntityKey, ProjectEntityLabelValue> projectEntityLabelTable() {
        return getTable(builderSingleton.builder,
                outputTopicNames.projectEntityLabel(),
                avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityLabelValue());
    }

    public KStream<ProjectStatementKey, ProjectStatementValue> projectStatementWithLiteralStream() {
        return getStream(builderSingleton.builder,
                outputTopicNames.projectStatementWithLiteral(),
                avroSerdes.ProjectStatementKey(), avroSerdes.ProjectStatementValue());
    }

    public KStream<ProjectTopStatementsKey, ProjectTopStatementsValue> projectTopOutgoingStatementsStream() {
        return getStream(builderSingleton.builder,
                outputTopicNames.projectTopOutgoingStatements(),
                avroSerdes.ProjectTopStatementsKey(), avroSerdes.ProjectTopStatementsValue());
    }

    public KStream<ProjectTopStatementsKey, ProjectTopStatementsValue> projectTopIncomingStatementsStream() {
        return getStream(builderSingleton.builder,
                outputTopicNames.projectTopIncomingStatements(),
                avroSerdes.ProjectTopStatementsKey(), avroSerdes.ProjectTopStatementsValue());
    }

    public KTable<ProjectTopStatementsKey, ProjectTopStatementsValue> projectTopStatementsTable() {
        return getTable(builderSingleton.builder,
                outputTopicNames.projectTopStatements(),
                avroSerdes.ProjectTopStatementsKey(), avroSerdes.ProjectTopStatementsValue());
    }

    public KStream<ProjectEntityKey, ProjectEntityVisibilityValue> projectEntityVisibilityStream() {
        return getStream(builderSingleton.builder,
                outputTopicNames.projectEntityVisibility(),
                avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityVisibilityValue());
    }

    public KStream<ProjectStatementKey, ProjectStatementValue> projectStatementWithEntityStream() {
        return getStream(builderSingleton.builder,
                outputTopicNames.projectStatementWithEntity(),
                avroSerdes.ProjectStatementKey(), avroSerdes.ProjectStatementValue());
    }

    public KTable<CommunityStatementKey, CommunityStatementValue> communityToolboxStatementWithEntityTable() {
        return getTable(builderSingleton.builder,
                outputTopicNames.communityToolboxStatementWithEntity(),
                avroSerdes.CommunityStatementKey(), avroSerdes.CommunityStatementValue());
    }

    public KStream<CommunityStatementKey, CommunityStatementValue> communityToolboxStatementWithLiteralStream() {
        return getStream(builderSingleton.builder,
                outputTopicNames.communityToolboxStatementWithLiteral(),
                avroSerdes.CommunityStatementKey(), avroSerdes.CommunityStatementValue());
    }

    public KTable<CommunityEntityKey, CommunityEntityLabelValue> communityToolboxEntityLabelTable() {
        return getTable(builderSingleton.builder,
                outputTopicNames.communityToolboxEntityLabel(),
                avroSerdes.CommunityEntityKey(), avroSerdes.CommunityEntityLabelValue());
    }

    public KStream<CommunityTopStatementsKey, CommunityTopStatementsValue> communityToolboxTopIncomingStatementsStream() {
        return getStream(builderSingleton.builder,
                outputTopicNames.communityToolboxTopIncomingStatements(),
                avroSerdes.CommunityTopStatementsKey(), avroSerdes.CommunityTopStatementsValue());
    }

    public KStream<CommunityTopStatementsKey, CommunityTopStatementsValue> communityToolboxTopOutgoingStatementsStream() {
        return getStream(builderSingleton.builder,
                outputTopicNames.communityToolboxTopOutgoingStatements(),
                avroSerdes.CommunityTopStatementsKey(), avroSerdes.CommunityTopStatementsValue());
    }

    public KTable<CommunityEntityKey, CommunityEntityValue> communityToolboxEntityTable() {
        return getTable(builderSingleton.builder,
                outputTopicNames.communityToolboxEntity(),
                avroSerdes.CommunityEntityKey(), avroSerdes.CommunityEntityValue());
    }

    public KTable<CommunityTopStatementsKey, CommunityTopStatementsValue> communityToolboxTopStatementsTable() {
        return getTable(builderSingleton.builder,
                outputTopicNames.communityToolboxTopStatements(),
                avroSerdes.CommunityTopStatementsKey(), avroSerdes.CommunityTopStatementsValue());
    }

}

