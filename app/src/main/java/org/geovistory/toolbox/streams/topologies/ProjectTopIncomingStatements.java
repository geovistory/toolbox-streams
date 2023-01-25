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
import org.geovistory.toolbox.streams.utils.TopStatementAdder;

import java.util.ArrayList;
import java.util.List;


public class ProjectTopIncomingStatements {
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
                                .setEntityId(value.getStatement().getObjectId())
                                .setPropertyId(value.getStatement().getPropertyId())
                                .setIsOutgoing(false)
                                .build(),
                        value
                ),
                Grouped
                        .with(avroSerdes.ProjectTopStatementsKey(), avroSerdes.ProjectStatementValue())
                        .withName(inner.TOPICS.project_top_incoming_statements_group_by)
        );
        // 3
        var aggregatedTable = grouped.aggregate(
                () -> ProjectTopStatementsValue.newBuilder()
                        .setProjectId(0)
                        .setEntityId("")
                        .setPropertyId(0)
                        .setStatements(new ArrayList<>())
                        .setIsOutgoing(false)
                        .build(),
                (aggKey, newValue, aggValue) -> {
                    aggValue.setProjectId(aggKey.getProjectId());
                    aggValue.setEntityId(aggKey.getEntityId());
                    aggValue.setPropertyId(aggKey.getPropertyId());
                    List<ProjectStatementValue> statements = aggValue.getStatements();
                    var newStatements = TopStatementAdder.addStatement(statements, newValue, false);
                    aggValue.setStatements(newStatements);
                    return aggValue;
                },
                (aggKey, oldValue, aggValue) -> aggValue,
                Materialized.<ProjectTopStatementsKey, ProjectTopStatementsValue, KeyValueStore<Bytes, byte[]>>as("project_top_incoming_statements_aggregate")
                        .withKeySerde(avroSerdes.ProjectTopStatementsKey())
                        .withValueSerde(avroSerdes.ProjectTopStatementsValue())
        );


        var aggregatedStream = aggregatedTable.toStream();

        /* SINK PROCESSORS */

        aggregatedStream.to(output.TOPICS.project_top_incoming_statements,
                Produced.with(avroSerdes.ProjectTopStatementsKey(), avroSerdes.ProjectTopStatementsValue()));

        return new ProjectTopStatementsReturnValue(builder, aggregatedTable, aggregatedStream);

    }


    public enum input {
        TOPICS;
        public final String project_statement = ProjectStatement.output.TOPICS.project_statement;
    }


    public enum inner {
        TOPICS;
        public final String project_top_incoming_statements_group_by = "project_top_incoming_statements_group_by";
    }

    public enum output {
        TOPICS;
        public final String project_top_incoming_statements = Utils.tsPrefixed("project_top_incoming_statements");
    }

}
