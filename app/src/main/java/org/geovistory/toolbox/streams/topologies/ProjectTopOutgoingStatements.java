package org.geovistory.toolbox.streams.topologies;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.app.RegisterOutputTopic;
import org.geovistory.toolbox.streams.avro.ProjectStatementKey;
import org.geovistory.toolbox.streams.avro.ProjectStatementValue;
import org.geovistory.toolbox.streams.avro.ProjectTopStatementsKey;
import org.geovistory.toolbox.streams.avro.ProjectTopStatementsValue;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.ArrayList;
import java.util.List;


public class ProjectTopOutgoingStatements {
    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerOutputTopic = new RegisterOutputTopic(builder);

        return addProcessors(
                builder,
                registerOutputTopic.projectStatementTable()
        ).builder().build();
    }

    //    public static ProjectStatementReturnValue addProcessors(
    public static ProjectTopStatementsReturnValue addProcessors(
            StreamsBuilder builder,
            KTable<ProjectStatementKey, ProjectStatementValue> projectStatementTable) {

        var avroSerdes = new ConfluentAvroSerdes();


        /* STREAM PROCESSORS */
        // 2)

        var grouped = projectStatementTable.groupBy(
                (key, value) -> KeyValue.pair(
                        ProjectTopStatementsKey.newBuilder()
                                .setProjectId(value.getProjectId())
                                .setEntityId(value.getStatement().getSubjectId())
                                .setPropertyId(value.getStatement().getPropertyId())
                                .setIsOutgoing(true)
                                .build(),
                        value
                ),
                Grouped
                        .with(avroSerdes.ProjectTopStatementsKey(), avroSerdes.ProjectStatementValue())
                        .withName(inner.TOPICS.project_top_outgoing_statements_group_by)
        );
        // 3
        var aggregatedTable = grouped.aggregate(
                () -> ProjectTopStatementsValue.newBuilder()
                        .setProjectId(0)
                        .setEntityId("")
                        .setPropertyId(0)
                        .setStatements(new ArrayList<>())
                        .setIsOutgoing(true)
                        .build(),
                (aggKey, newValue, aggValue) -> {
                    aggValue.setProjectId(aggKey.getProjectId());
                    aggValue.setEntityId(aggKey.getEntityId());
                    aggValue.setPropertyId(aggKey.getProjectId());
                    aggValue.setProjectId(aggKey.getProjectId());
                    List<ProjectStatementValue> statements = aggValue.getStatements();
                    var newStatements = addStatement(statements, newValue);
                    aggValue.setStatements(newStatements);
                    return aggValue;
                },
                (aggKey, oldValue, aggValue) -> aggValue,
                Materialized.<ProjectTopStatementsKey, ProjectTopStatementsValue, KeyValueStore<Bytes, byte[]>>as("project_top_outgoing_statements_aggregate")
                        .withKeySerde(avroSerdes.ProjectTopStatementsKey())
                        .withValueSerde(avroSerdes.ProjectTopStatementsValue())
        );


        var aggregatedStream = aggregatedTable.toStream();

        /* SINK PROCESSORS */

        aggregatedStream.to(output.TOPICS.project_top_outgoing_statements,
                Produced.with(avroSerdes.ProjectTopStatementsKey(), avroSerdes.ProjectTopStatementsValue()));

        return new ProjectTopStatementsReturnValue(builder, aggregatedTable, aggregatedStream);

    }

    public static List<ProjectStatementValue> addStatement(List<ProjectStatementValue> existingList, ProjectStatementValue newItem) {
        int targetPosition = -1;
        ProjectStatementValue replacedItem = null;


        for (int i = 0; i < existingList.size(); i++) {
            var oldItem = existingList.get(i);
            var oldOrdNum = oldItem.getOrdNumForDomain();
            var newOrdNum = newItem.getOrdNumForDomain();
            var oldId = oldItem.getStatementId();
            var newId = newItem.getStatementId();
            // if the new item should be before old item...
            if (newOrdNum != null && oldOrdNum != null && newOrdNum <= oldOrdNum && targetPosition == -1) {
                // ...set the target position of the new item
                targetPosition = i;
            }
            // if the newItem replaces an oldItem...
            if (oldId == newId) {
                // ...keep track of this old item
                replacedItem = oldItem;
            }
        }

        // if item was deleted...
        if (newItem.getDeleted$1() != null && newItem.getDeleted$1()) {
            // ...remove it
            if (replacedItem != null) existingList.remove(replacedItem);
            // ...and return immediately
            return existingList;
        }

        // add item at retrieved position
        if (targetPosition > -1) {
            existingList.add(targetPosition, newItem);
        }
        // append item to the end
        else if (existingList.size() < 5) {
            existingList.add(newItem);
        }

        // remove the replaced item
        if (replacedItem != null) {
            existingList.remove(replacedItem);
        }

        // keep max. list size
        if (existingList.size() > 5) existingList.remove(5);

        return existingList;
    }


    public enum input {
        TOPICS;
        public final String project_statement = ProjectStatement.output.TOPICS.project_statement;
    }


    public enum inner {
        TOPICS;
        public final String project_top_outgoing_statements_group_by = "project_top_outgoing_statements_group_by";
    }

    public enum output {
        TOPICS;
        public final String project_top_outgoing_statements = Utils.tsPrefixed("project_top_outgoing_statements");
    }

}