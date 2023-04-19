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
public class CommunityToolboxTopIncomingStatements {

    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;
    @Inject
    RegisterInnerTopic registerInnerTopic;

    @Inject
    OutputTopicNames outputTopicNames;

    public CommunityToolboxTopIncomingStatements(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, RegisterInnerTopic registerInnerTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.registerInnerTopic = registerInnerTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {


        addProcessors(
                registerInnerTopic.communityToolboxStatementWithEntityTable(),
                registerInnerTopic.communityToolboxEntityLabelTable()
        );
    }

    public CommunityTopStatementsReturnValue addProcessors(
            KTable<CommunityStatementKey, CommunityStatementValue> communityToolboxStatementWithEntityTable,
            KTable<CommunityEntityKey, CommunityEntityLabelValue> communityToolboxEntityLabelTable) {

        /* STREAM PROCESSORS */
        // 2)
        // join subject entity labels to get subject label
        var joinedSubjectEntityLabelsTable = communityToolboxStatementWithEntityTable.join(
                communityToolboxEntityLabelTable,
                communityToolboxStatementValue -> CommunityEntityKey.newBuilder()
                        .setEntityId(communityToolboxStatementValue.getStatement().getSubjectId())
                        .build(),
                (communityToolboxStatementValue, communityToolboxEntityLabelValue) -> {
                    if (communityToolboxEntityLabelValue != null && communityToolboxEntityLabelValue.getLabel() != null) {
                        communityToolboxStatementValue.getStatement().setSubjectLabel(communityToolboxEntityLabelValue.getLabel());
                    }
                    return communityToolboxStatementValue;
                },
                TableJoined.as(inner.TOPICS.community_toolbox_top_incoming_statements_join_subject_entity_label + "-fk-join"),
                Materialized.<CommunityStatementKey, CommunityStatementValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.community_toolbox_top_incoming_statements_join_subject_entity_label)
                        .withKeySerde(avroSerdes.CommunityStatementKey())
                        .withValueSerde(avroSerdes.CommunityStatementValue())
        );

        // 4
        var grouped = joinedSubjectEntityLabelsTable
                .toStream(
                        Named.as(inner.TOPICS.community_toolbox_top_incoming_statements_join_subject_entity_label + "-to-stream")
                )
                .groupBy(
                        (key, value) ->
                                CommunityTopStatementsKey.newBuilder()
                                        .setEntityId(value.getStatement().getObjectId())
                                        .setPropertyId(value.getStatement().getPropertyId())
                                        .setIsOutgoing(false)
                                        .build(),
                        Grouped
                                .with(avroSerdes.CommunityTopStatementsKey(), avroSerdes.CommunityStatementValue())
                                .withName(inner.TOPICS.community_toolbox_top_incoming_statements_group_by)
                );
        // 5
        var aggregatedTable = grouped.aggregate(
                () -> CommunityTopStatementsValue.newBuilder()
                        .setPropertyId(0)
                        .setEntityId("")
                        .setStatements(new ArrayList<>())
                        .setIsOutgoing(false)
                        .build(),
                (aggKey, newValue, aggValue) -> {
                    aggValue.setEntityId(aggKey.getEntityId());
                    aggValue.setPropertyId(aggKey.getPropertyId());
                    List<CommunityStatementValue> statements = aggValue.getStatements();
                    var newStatements = CommunityTopStatementAdder.addStatement(statements, newValue, false);
                    // extract class id of entity from new statements, if there are, or from old, if there are
                    var stmts = newStatements.size() > 0 ? newStatements : aggValue.getStatements();
                    if (stmts.size() > 0) {
                        var firstStatement = newStatements.get(0).getStatement();
                        aggValue.setClassId(firstStatement.getObjectClassId());
                    }
                    aggValue.setStatements(newStatements);
                    return aggValue;
                },
                Materialized.<CommunityTopStatementsKey, CommunityTopStatementsValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.community_toolbox_top_incoming_statements_aggregate)
                        .withKeySerde(avroSerdes.CommunityTopStatementsKey())
                        .withValueSerde(avroSerdes.CommunityTopStatementsValue())
        );


        var aggregatedStream = aggregatedTable.toStream(
                Named.as(inner.TOPICS.community_toolbox_top_incoming_statements_aggregate + "-to-stream")
        );

        /* SINK PROCESSORS */

        aggregatedStream.to(outputTopicNames.communityToolboxTopIncomingStatements(),
                Produced.with(avroSerdes.CommunityTopStatementsKey(), avroSerdes.CommunityTopStatementsValue())
                        .withName(outputTopicNames.communityToolboxTopIncomingStatements() + "-producer")
        );

        return new CommunityTopStatementsReturnValue(aggregatedTable, aggregatedStream);

    }

    public enum inner {
        TOPICS;
        public final String community_toolbox_top_incoming_statements_group_by = "community_toolbox_top_incoming_statements_group_by";
        public final String community_toolbox_top_incoming_statements_aggregate = "community_toolbox_top_incoming_statements_aggregate";
        public final String community_toolbox_top_incoming_statements_join_subject_entity_label = "community_toolbox_top_incoming_statements_join_subject_entity_label";

    }

}
