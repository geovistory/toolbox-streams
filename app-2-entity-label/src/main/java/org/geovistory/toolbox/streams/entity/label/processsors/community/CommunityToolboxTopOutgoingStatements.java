package org.geovistory.toolbox.streams.entity.label.processsors.community;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.label.AvroSerdes;
import org.geovistory.toolbox.streams.entity.label.OutputTopicNames;
import org.geovistory.toolbox.streams.entity.label.RegisterInnerTopic;
import org.geovistory.toolbox.streams.entity.label.RegisterInputTopic;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;


@ApplicationScoped
public class CommunityToolboxTopOutgoingStatements {


    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;
    @Inject
    RegisterInnerTopic registerInnerTopic;

    @Inject
    OutputTopicNames outputTopicNames;

    public CommunityToolboxTopOutgoingStatements(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, RegisterInnerTopic registerInnerTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.registerInnerTopic = registerInnerTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {


        addProcessors(
                registerInnerTopic.communityToolboxStatementWithLiteralStream(),
                registerInnerTopic.communityToolboxStatementWithEntityTable(),
                registerInnerTopic.communityToolboxEntityLabelTable()
        );
    }

    public CommunityTopStatementsReturnValue addProcessors(
            KStream<CommunityStatementKey, CommunityStatementValue> communityToolboxStatementsWithLiteralStream,
            KTable<CommunityStatementKey, CommunityStatementValue> communityToolboxStatementsWithEntityTable,
            KTable<CommunityEntityKey, CommunityEntityLabelValue> communityToolboxEntityLabelTable) {


        /* STREAM PROCESSORS */
        // 2)
        // join object entity labels to get object label
        var joinedObjectEntityLabelsTable = communityToolboxStatementsWithEntityTable.leftJoin(communityToolboxEntityLabelTable,
                communityToolboxStatementValue -> CommunityEntityKey.newBuilder()
                        .setEntityId(communityToolboxStatementValue.getStatement().getObjectId())
                        .build(),
                (communityToolboxStatementValue, communityToolboxEntityLabelValue) -> {
                    if (communityToolboxEntityLabelValue != null && communityToolboxEntityLabelValue.getLabel() != null) {
                        communityToolboxStatementValue.getStatement().setObjectLabel(communityToolboxEntityLabelValue.getLabel());
                    }
                    return communityToolboxStatementValue;
                },
                TableJoined.as(inner.TOPICS.community_toolbox_top_outgoing_statements_join_object_entity_label + "-fk-left-join"),
                Materialized.<CommunityStatementKey, CommunityStatementValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.community_toolbox_top_outgoing_statements_join_object_entity_label)
                        .withKeySerde(avroSerdes.CommunityStatementKey())
                        .withValueSerde(avroSerdes.CommunityStatementValue()));
        // 3
        var statementsStream = joinedObjectEntityLabelsTable.toStream(
                Named.as(inner.TOPICS.community_toolbox_top_outgoing_statements_join_object_entity_label + "-to-stream")
        ).merge(
                communityToolboxStatementsWithLiteralStream,
                Named.as("kstream-merge-communityToolbox-top-out-s-with-entity-label-and-communityToolbox-top-out-s-with-literal")
        );

        // 4
        var grouped = statementsStream.groupBy(
                (key, value) ->
                        CommunityTopStatementsKey.newBuilder()
                                .setEntityId(value.getStatement().getSubjectId())
                                .setPropertyId(value.getStatement().getPropertyId())
                                .setIsOutgoing(true)
                                .build(),
                Grouped
                        .with(avroSerdes.CommunityTopStatementsKey(), avroSerdes.CommunityStatementValue())
                        .withName(inner.TOPICS.community_toolbox_top_outgoing_statements_group_by)
        );

        // 5
        var aggregatedTable = grouped.aggregate(
                () -> CommunityTopStatementsValue.newBuilder()
                        .setEntityId("")
                        .setPropertyId(0)
                        .setStatements(new ArrayList<>())
                        .setIsOutgoing(true)
                        .build(),
                (aggKey, newValue, aggValue) -> {
                    aggValue.setEntityId(aggKey.getEntityId());
                    aggValue.setPropertyId(aggKey.getPropertyId());
                    List<CommunityStatementValue> statements = aggValue.getStatements();
                    var newStatements = CommunityTopStatementAdder.addStatement(statements, newValue, true);
                    // extract class id of entity from new statements, if there are, or from old, if there are
                    var stmts = newStatements.size() > 0 ? newStatements : aggValue.getStatements();
                    if (stmts.size() > 0) {
                        var firstStatement = newStatements.get(0).getStatement();
                        aggValue.setClassId(firstStatement.getSubjectClassId());
                    }
                    aggValue.setStatements(newStatements);
                    return aggValue;
                },
                Named.as("community_toolbox_top_outgoing_statements_aggregate"),
                Materialized.<CommunityTopStatementsKey, CommunityTopStatementsValue, KeyValueStore<Bytes, byte[]>>as("community_toolbox_top_outgoing_statements_aggregate")
                        .withKeySerde(avroSerdes.CommunityTopStatementsKey())
                        .withValueSerde(avroSerdes.CommunityTopStatementsValue())
        );


        var aggregatedStream = aggregatedTable.toStream(
                Named.as("community_toolbox_top_outgoing_statements_aggregate" + "-to-stream")
        );

        /* SINK PROCESSORS */

        aggregatedStream.to(outputTopicNames.communityToolboxTopOutgoingStatements(),
                Produced.with(avroSerdes.CommunityTopStatementsKey(), avroSerdes.CommunityTopStatementsValue())
                        .withName(outputTopicNames.communityToolboxTopOutgoingStatements() + "-producer")
        );

        return new CommunityTopStatementsReturnValue(aggregatedTable, aggregatedStream);

    }


    public enum inner {
        TOPICS;
        public final String community_toolbox_top_outgoing_statements_group_by = "community_toolbox_top_outgoing_statements_group_by";
        public final String community_toolbox_top_outgoing_statements_join_object_entity_label = "community_toolbox_top_outgoing_statements_join_object_entity_label";
    }

}
