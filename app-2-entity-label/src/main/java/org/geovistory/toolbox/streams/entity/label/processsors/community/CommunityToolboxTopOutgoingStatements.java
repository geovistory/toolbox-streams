package org.geovistory.toolbox.streams.entity.label.processsors.community;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.label.RegisterInnerTopic;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.ArrayList;
import java.util.List;


public class CommunityToolboxTopOutgoingStatements {
    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerOutputTopic = new RegisterInnerTopic(builder);

        return addProcessors(
                builder,
                registerOutputTopic.communityToolboxStatementWithLiteralStream(),
                registerOutputTopic.communityToolboxStatementWithEntityTable(),
                registerOutputTopic.communityToolboxEntityLabelTable()
        ).builder().build();
    }

    public static CommunityTopStatementsReturnValue addProcessors(
            StreamsBuilder builder,
            KStream<CommunityStatementKey, CommunityStatementValue> communityToolboxStatementsWithLiteralStream,
            KTable<CommunityStatementKey, CommunityStatementValue> communityToolboxStatementsWithEntityTable,
            KTable<CommunityEntityKey, CommunityEntityLabelValue> communityToolboxEntityLabelTable) {

        var avroSerdes = new ConfluentAvroSerdes();


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

        aggregatedStream.to(output.TOPICS.community_toolbox_top_outgoing_statements,
                Produced.with(avroSerdes.CommunityTopStatementsKey(), avroSerdes.CommunityTopStatementsValue())
                        .withName(output.TOPICS.community_toolbox_top_outgoing_statements + "-producer")
        );

        return new CommunityTopStatementsReturnValue(builder, aggregatedTable, aggregatedStream);

    }


    public enum input {
        TOPICS;
        public final String community_toolbox_statement = CommunityToolboxStatementWithEntity.output.TOPICS.community_toolbox_statement_with_entity;
        public final String community_toolbox_entity_label = CommunityToolboxEntityLabel.output.TOPICS.community_toolbox_entity_label;

    }


    public enum inner {
        TOPICS;
        public final String community_toolbox_top_outgoing_statements_group_by = "community_toolbox_top_outgoing_statements_group_by";
        public final String community_toolbox_top_outgoing_statements_join_object_entity_label = "community_toolbox_top_outgoing_statements_join_object_entity_label";

    }

    public enum output {
        TOPICS;
        public final String community_toolbox_top_outgoing_statements = Utils.tsPrefixed("community_toolbox_top_outgoing_statements");
    }

}
