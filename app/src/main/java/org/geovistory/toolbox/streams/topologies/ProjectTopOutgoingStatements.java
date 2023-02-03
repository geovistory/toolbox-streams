package org.geovistory.toolbox.streams.topologies;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.app.RegisterOutputTopic;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.utils.TopStatementAdder;

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
                registerOutputTopic.projectStatementWithLiteralStream(),
                registerOutputTopic.projectStatementWithEntityTable(),
                registerOutputTopic.projectEntityLabelTable()
        ).builder().build();
    }

    public static ProjectTopStatementsReturnValue addProcessors(
            StreamsBuilder builder,
            KStream<ProjectStatementKey, ProjectStatementValue> projectStatementsWithLiteralStream,
            KTable<ProjectStatementKey, ProjectStatementValue> projectStatementsWithEntityTable,
            KTable<ProjectEntityKey, ProjectEntityLabelValue> projectEntityLabelTable) {

        var avroSerdes = new ConfluentAvroSerdes();


        /* STREAM PROCESSORS */
        // 2)

        // join object entity labels to get object label
        var joinedObjectEntityLabelsTable = projectStatementsWithEntityTable.leftJoin(projectEntityLabelTable,
                projectStatementValue -> ProjectEntityKey.newBuilder()
                        .setEntityId(projectStatementValue.getStatement().getObjectId())
                        .setProjectId(projectStatementValue.getProjectId())
                        .build(),
                (projectStatementValue, projectEntityLabelValue) -> {
                    if (projectEntityLabelValue != null && projectEntityLabelValue.getLabel() != null) {
                        projectStatementValue.getStatement().setObjectLabel(projectEntityLabelValue.getLabel());
                    }
                    return projectStatementValue;
                },
                Materialized.<ProjectStatementKey, ProjectStatementValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_top_outgoing_statements_join_object_entity_label)
                        .withKeySerde(avroSerdes.ProjectStatementKey())
                        .withValueSerde(avroSerdes.ProjectStatementValue()));
        // 3
        var statementsStream = joinedObjectEntityLabelsTable.toStream().merge(projectStatementsWithLiteralStream);
        var statementsTable = statementsStream.toTable(
                Named.as(inner.TOPICS.project_outgoing_statements),
                Materialized.with(avroSerdes.ProjectStatementKey(), avroSerdes.ProjectStatementValue())
        );
        // 3
        var grouped = statementsTable.groupBy(
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

        // 4
        var aggregatedTable = grouped.aggregate(
                () -> ProjectTopStatementsValue.newBuilder()
                        .setProjectId(0)
                        .setEntityId("")
                        .setPropertyId(0)
                        .setStatements(new ArrayList<>())
                        .setIsOutgoing(true)
                        .build(),
                (aggKey, newValue, aggValue) -> {
                    aggValue.setEntityId(aggKey.getEntityId());
                    aggValue.setPropertyId(aggKey.getPropertyId());
                    aggValue.setProjectId(aggKey.getProjectId());
                    List<ProjectStatementValue> statements = aggValue.getStatements();
                    var newStatements = TopStatementAdder.addStatement(statements, newValue, true);
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


    public enum input {
        TOPICS;
        public final String project_statement = ProjectStatementWithEntity.output.TOPICS.project_statement_with_entity;
        public final String project_entity_label = ProjectEntityLabel.output.TOPICS.project_entity_label;

    }


    public enum inner {
        TOPICS;
        public final String project_top_outgoing_statements_group_by = "project_top_outgoing_statements_group_by";
        public final String project_top_outgoing_statements_join_object_entity_label = "project_top_outgoing_statements_join_object_entity_label";
        public final String project_outgoing_statements = "project_outgoing_statements";

    }

    public enum output {
        TOPICS;
        public final String project_top_outgoing_statements = Utils.tsPrefixed("project_top_outgoing_statements");
    }

}
