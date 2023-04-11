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
public class ProjectTopOutgoingStatements {
    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;
    @Inject
    RegisterInnerTopic registerInnerTopic;

    @Inject
    OutputTopicNames outputTopicNames;

    public ProjectTopOutgoingStatements(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, RegisterInnerTopic registerInnerTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.registerInnerTopic = registerInnerTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {

        addProcessors(
                registerInnerTopic.projectStatementWithLiteralStream(),
                registerInnerTopic.projectStatementWithEntityTable(),
                registerInnerTopic.projectEntityLabelTable()
        );
    }

    public ProjectTopStatementsReturnValue addProcessors(
            KStream<ProjectStatementKey, ProjectStatementValue> projectStatementsWithLiteralStream,
            KTable<ProjectStatementKey, ProjectStatementValue> projectStatementsWithEntityTable,
            KTable<ProjectEntityKey, ProjectEntityLabelValue> projectEntityLabelTable) {



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

                    // extract class id of entity from new statements, if there are, or from old, if there are
                    var stmts = newStatements.size() > 0 ? newStatements : aggValue.getStatements();
                    if (stmts.size() > 0) {
                        var firstStatement = newStatements.get(0).getStatement();
                        aggValue.setClassId(firstStatement.getSubjectClassId());

                    }

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

        aggregatedStream.to(outputTopicNames.projectTopOutgoingStatements(),
                Produced.with(avroSerdes.ProjectTopStatementsKey(), avroSerdes.ProjectTopStatementsValue())
                        .withName(outputTopicNames.projectTopOutgoingStatements() + "-producer")
        );

        return new ProjectTopStatementsReturnValue(aggregatedTable, aggregatedStream);

    }

    public enum inner {
        TOPICS;
        public final String project_top_outgoing_statements_group_by = "project_top_outgoing_statements_group_by";
        public final String project_top_outgoing_statements_join_object_entity_label = "project_top_outgoing_statements_join_object_entity_label";

    }

}
