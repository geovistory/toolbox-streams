package org.geovistory.toolbox.streams.entity.label.processsors.project;

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
public class ProjectTopIncomingStatements {

    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;
    @Inject
    RegisterInnerTopic registerInnerTopic;

    @Inject
    OutputTopicNames outputTopicNames;

    public ProjectTopIncomingStatements(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, RegisterInnerTopic registerInnerTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.registerInnerTopic = registerInnerTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {

        addProcessors(
                registerInnerTopic.projectStatementWithEntityTable(),
                registerInnerTopic.projectEntityLabelTable()
        );
    }

    public ProjectTopStatementsReturnValue addProcessors(
            KTable<ProjectStatementKey, ProjectStatementValue> projectStatementWithEntityTable,
            KTable<ProjectEntityKey, ProjectEntityLabelValue> projectEntityLabelTable) {



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

        aggregatedStream.to(outputTopicNames.projectTopIncomingStatements(),
                Produced.with(avroSerdes.ProjectTopStatementsKey(), avroSerdes.ProjectTopStatementsValue())
                        .withName(outputTopicNames.projectTopIncomingStatements() + "-producer")
        );

        return new ProjectTopStatementsReturnValue(aggregatedTable, aggregatedStream);

    }


    public enum inner {
        TOPICS;
        public final String project_top_incoming_statements_group_by = "project_top_incoming_statements_group_by";
        public final String project_top_incoming_statements_aggregate = "project_top_incoming_statements_aggregate";
        public final String project_top_incoming_statements_join_subject_entity_label = "project_top_incoming_statements_join_subject_entity_label";

    }


}
