package org.geovistory.toolbox.streams.entity.label.processsors.project;

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


public class ProjectTopIncomingStatements {
    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerOutputTopic = new RegisterInnerTopic(builder);

        return addProcessors(
                builder,
                registerOutputTopic.projectStatementWithEntityTable(),
                registerOutputTopic.projectEntityLabelTable()
        ).builder().build();
    }

    public static ProjectTopStatementsReturnValue addProcessors(
            StreamsBuilder builder,
            KTable<ProjectStatementKey, ProjectStatementValue> projectStatementWithEntityTable,
            KTable<ProjectEntityKey, ProjectEntityLabelValue> projectEntityLabelTable) {

        var avroSerdes = new ConfluentAvroSerdes();


        /* STREAM PROCESSORS */
        // 2)
        // join subject entity labels to get subject label
        var joinedSubjectEntityLabelsTable = projectStatementWithEntityTable.join(
                projectEntityLabelTable,
                projectStatementValue -> ProjectEntityKey.newBuilder()
                        .setEntityId(projectStatementValue.getStatement().getSubjectId())
                        .setProjectId(projectStatementValue.getProjectId())
                        .build(),
                (projectStatementValue, projectEntityLabelValue) -> {
                    if (projectEntityLabelValue != null && projectEntityLabelValue.getLabel() != null) {
                        projectStatementValue.getStatement().setSubjectLabel(projectEntityLabelValue.getLabel());
                    }
                    return projectStatementValue;
                },
                TableJoined.as(inner.TOPICS.project_top_incoming_statements_join_subject_entity_label + "-fk-join"),
                Materialized.<ProjectStatementKey, ProjectStatementValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_top_incoming_statements_join_subject_entity_label)
                        .withKeySerde(avroSerdes.ProjectStatementKey())
                        .withValueSerde(avroSerdes.ProjectStatementValue())
        );

        // 4
        var grouped = joinedSubjectEntityLabelsTable
                .toStream(
                        Named.as(inner.TOPICS.project_top_incoming_statements_join_subject_entity_label + "-to-stream")
                )
                .groupBy(
                        (key, value) ->
                                ProjectTopStatementsKey.newBuilder()
                                        .setProjectId(value.getProjectId())
                                        .setEntityId(value.getStatement().getObjectId())
                                        .setPropertyId(value.getStatement().getPropertyId())
                                        .setIsOutgoing(false)
                                        .build(),
                        Grouped
                                .with(avroSerdes.ProjectTopStatementsKey(), avroSerdes.ProjectStatementValue())
                                .withName(inner.TOPICS.project_top_incoming_statements_group_by)
                );
        // 5
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
                    // extract class id of entity from new statements, if there are, or from old, if there are
                    var stmts = newStatements.size() > 0 ? newStatements : aggValue.getStatements();
                    if (stmts.size() > 0) {
                        var firstStatement = newStatements.get(0).getStatement();
                        aggValue.setClassId(firstStatement.getObjectClassId());
                    }
                    aggValue.setStatements(newStatements);
                    return aggValue;
                },
                Materialized.<ProjectTopStatementsKey, ProjectTopStatementsValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_top_incoming_statements_aggregate)
                        .withKeySerde(avroSerdes.ProjectTopStatementsKey())
                        .withValueSerde(avroSerdes.ProjectTopStatementsValue())
        );


        var aggregatedStream = aggregatedTable.toStream(
                Named.as(inner.TOPICS.project_top_incoming_statements_aggregate + "-to-stream")
        );

        /* SINK PROCESSORS */

        aggregatedStream.to(output.TOPICS.project_top_incoming_statements,
                Produced.with(avroSerdes.ProjectTopStatementsKey(), avroSerdes.ProjectTopStatementsValue())
                        .withName(output.TOPICS.project_top_incoming_statements + "-producer")
        );

        return new ProjectTopStatementsReturnValue(builder, aggregatedTable, aggregatedStream);

    }


    public enum input {
        TOPICS;
        public final String project_statement_with_entity = ProjectStatementWithEntity.output.TOPICS.project_statement_with_entity;
        public final String project_entity_label = ProjectEntityLabel.output.TOPICS.project_entity_label;
    }


    public enum inner {
        TOPICS;
        public final String project_top_incoming_statements_group_by = "project_top_incoming_statements_group_by";
        public final String project_top_incoming_statements_aggregate = "project_top_incoming_statements_aggregate";
        public final String project_top_incoming_statements_join_subject_entity_label = "project_top_incoming_statements_join_subject_entity_label";

    }

    public enum output {
        TOPICS;
        public final String project_top_incoming_statements = Utils.tsPrefixed("project_top_incoming_statements");
    }

}
