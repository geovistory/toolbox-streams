package org.geovistory.toolbox.streams.project.entity.label.processors;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.project.entity.label.RegisterInnerTopic;

import java.util.ArrayList;
import java.util.List;


public class ProjectTopOutgoingStatements {
    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerOutputTopic = new RegisterInnerTopic(builder);

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
                TableJoined.as(inner.TOPICS.project_top_outgoing_statements_join_object_entity_label + "-fk-left-join"),
                Materialized.<ProjectStatementKey, ProjectStatementValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_top_outgoing_statements_join_object_entity_label)
                        .withKeySerde(avroSerdes.ProjectStatementKey())
                        .withValueSerde(avroSerdes.ProjectStatementValue()));
        // 3
        var statementsStream = joinedObjectEntityLabelsTable.toStream(
                Named.as(inner.TOPICS.project_top_outgoing_statements_join_object_entity_label + "-to-stream")
        ).merge(
                projectStatementsWithLiteralStream,
                Named.as("kstream-merge-project-top-out-s-with-entity-label-and-project-top-out-s-with-literal")
        );

        // 4
        var grouped = statementsStream.groupBy(
                (key, value) ->
                        ProjectTopStatementsKey.newBuilder()
                                .setProjectId(value.getProjectId())
                                .setEntityId(value.getStatement().getSubjectId())
                                .setPropertyId(value.getStatement().getPropertyId())
                                .setIsOutgoing(true)
                                .build(),
                Grouped
                        .with(avroSerdes.ProjectTopStatementsKey(), avroSerdes.ProjectStatementValue())
                        .withName(inner.TOPICS.project_top_outgoing_statements_group_by)
        );

        // 5
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
                Named.as("project_top_outgoing_statements_aggregate"),
                Materialized.<ProjectTopStatementsKey, ProjectTopStatementsValue, KeyValueStore<Bytes, byte[]>>as("project_top_outgoing_statements_aggregate")
                        .withKeySerde(avroSerdes.ProjectTopStatementsKey())
                        .withValueSerde(avroSerdes.ProjectTopStatementsValue())
        );


        var aggregatedStream = aggregatedTable.toStream(
                Named.as("project_top_outgoing_statements_aggregate" + "-to-stream")
        );

        /* SINK PROCESSORS */

        aggregatedStream.to(output.TOPICS.project_top_outgoing_statements,
                Produced.with(avroSerdes.ProjectTopStatementsKey(), avroSerdes.ProjectTopStatementsValue())
                        .withName(output.TOPICS.project_top_outgoing_statements + "-producer")
        );

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

    }

    public enum output {
        TOPICS;
        public final String project_top_outgoing_statements = Utils.tsPrefixed("project_top_outgoing_statements");
    }

}
