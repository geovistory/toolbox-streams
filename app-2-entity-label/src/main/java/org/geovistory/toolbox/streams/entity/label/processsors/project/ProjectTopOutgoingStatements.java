package org.geovistory.toolbox.streams.entity.label.processsors.project;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.label.AvroSerdes;
import org.geovistory.toolbox.streams.entity.label.OutputTopicNames;
import org.geovistory.toolbox.streams.entity.label.RegisterInnerTopic;
import org.geovistory.toolbox.streams.entity.label.RegisterInputTopic;
import org.geovistory.toolbox.streams.lib.Utils;

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
                registerInnerTopic.projectEntityLabelTable(),
                registerInnerTopic.communityToolboxEntityLabelTable()
        );
    }

    public ProjectTopStatementsReturnValue addProcessors(
            KStream<ProjectStatementKey, ProjectStatementValue> projectStatementsWithLiteralStream,
            KTable<ProjectStatementKey, ProjectStatementValue> projectStatementsWithEntityTable,
            KTable<ProjectEntityKey, ProjectEntityLabelValue> projectEntityLabelTable,
            KTable<CommunityEntityKey, CommunityEntityLabelValue> communityEntityLabelTable) {



        /* STREAM PROCESSORS */
        // 2)
        // join object entity labels to get object label
        var joinedProjectLabels = projectStatementsWithEntityTable.leftJoin(projectEntityLabelTable,
                projectStatementValue -> ProjectEntityKey.newBuilder()
                        .setEntityId(projectStatementValue.getStatement().getObjectId())
                        .setProjectId(projectStatementValue.getProjectId())
                        .build(),
                (statementValue, projectEntityLabelValue) -> {
                    var e = ProjectEdgeValue.newBuilder()
                            .setProjectId(statementValue.getProjectId())
                            .setStatementId(statementValue.getStatementId())
                            .setModifiedAt(statementValue.getModifiedAt())
                            .setOrdNum(statementValue.getOrdNumOfRange())
                            .setDeleted(Utils.booleanIsEqualTrue(statementValue.getDeleted$1()));


                    if (statementValue.getStatement() != null) {
                        var s = statementValue.getStatement();
                        e.setPropertyId(s.getPropertyId())
                                .setSourceId(s.getSubjectId())
                                .setSourceClassId(s.getSubjectClassId())
                                .setPropertyId(s.getPropertyId())
                                .setTargetId(s.getObjectId())
                                .setTargetNode(s.getObject());
                    }

                    String targetLabel = null;
                    if (projectEntityLabelValue != null && Utils.booleanIsNotEqualTrue(projectEntityLabelValue.getDeleted$1())) {
                        targetLabel = projectEntityLabelValue.getLabel();
                    }

                    e.setTargetLabel(targetLabel);

                    return e.build();
                },
                TableJoined.as(inner.TOPICS.project_top_outgoing_edges_join_project_entity_label + "-fk-left-join"),
                Materialized.<ProjectStatementKey, ProjectEdgeValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_top_outgoing_edges_join_project_entity_label)
                        .withKeySerde(avroSerdes.ProjectStatementKey())
                        .withValueSerde(avroSerdes.ProjectEdgeValue()));
        // 3
        // join community entity labels
        var joinedCommunityLabels = joinedProjectLabels.leftJoin(
                communityEntityLabelTable,
                projectEdgeValue -> CommunityEntityKey.newBuilder()
                        .setEntityId(projectEdgeValue.getTargetId()).build(),
                (projectEdgeValue, communityEntityLabelValue) -> {
                    var projectLabel = projectEdgeValue.getTargetLabel();
                    if (projectLabel == null || projectLabel.equals("")) {
                        String targetLabel = null;
                        if (communityEntityLabelValue != null && Utils.booleanIsNotEqualTrue(communityEntityLabelValue.getDeleted$1())) {
                            targetLabel = communityEntityLabelValue.getLabel();
                        }
                        projectEdgeValue.setTargetLabel(targetLabel);
                    }
                    return projectEdgeValue;
                },
                TableJoined.as(inner.TOPICS.project_top_outgoing_edges_join_community_entity_label + "-fk-join"),
                Materialized.<ProjectStatementKey, ProjectEdgeValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_top_outgoing_edges_join_community_entity_label)
                        .withKeySerde(avroSerdes.ProjectStatementKey())
                        .withValueSerde(avroSerdes.ProjectEdgeValue())
        );

        // 3
        var projectLiteralEdges = projectStatementsWithLiteralStream.mapValues((readOnlyKey, statementValue) -> {
                    var e = ProjectEdgeValue.newBuilder()
                            .setProjectId(statementValue.getProjectId())
                            .setStatementId(statementValue.getStatementId())
                            .setModifiedAt(statementValue.getModifiedAt())
                            .setOrdNum(statementValue.getOrdNumOfRange())
                            .setDeleted(Utils.booleanIsEqualTrue(statementValue.getDeleted$1()));
                    if (statementValue.getStatement() != null) {
                        var s = statementValue.getStatement();
                        e.setPropertyId(s.getPropertyId())
                                .setSourceId(s.getSubjectId())
                                .setSourceClassId(s.getSubjectClassId())
                                .setPropertyId(s.getPropertyId())
                                .setTargetId(s.getObjectId())
                                .setTargetLabel(s.getObjectLabel())
                                .setTargetNode(s.getObject());
                    }
                    return e.build();
                }
        );

        var statementsStream = joinedCommunityLabels.toStream(
                Named.as(inner.TOPICS.project_top_outgoing_edges_join_project_entity_label + "-to-stream")
        ).merge(
                projectLiteralEdges,
                Named.as("kstream-merge-project-top-out-s-with-entity-label-and-project-top-out-s-with-literal")
        );

        // 4
        var grouped = statementsStream.groupBy(
                (key, value) -> value == null ? null :
                        ProjectTopStatementsKey.newBuilder()
                                .setProjectId(value.getProjectId())
                                .setEntityId(value.getSourceId())
                                .setPropertyId(value.getPropertyId())
                                .setIsOutgoing(true)
                                .build(),
                Grouped
                        .with(avroSerdes.ProjectTopStatementsKey(), avroSerdes.ProjectEdgeValue())
                        .withName(inner.TOPICS.project_top_outgoing_edges_group_by)
        );

        // 5
        var aggregatedTable = grouped.aggregate(
                () -> ProjectTopStatementsValue.newBuilder()
                        .setProjectId(0)
                        .setEntityId("")
                        .setPropertyId(0)
                        .setEdges(new ArrayList<>())
                        .setIsOutgoing(true)
                        .build(),
                (aggKey, newValue, aggValue) -> {
                    aggValue.setEntityId(aggKey.getEntityId());
                    aggValue.setPropertyId(aggKey.getPropertyId());
                    aggValue.setProjectId(aggKey.getProjectId());
                    List<ProjectEdgeValue> statements = aggValue.getEdges();
                    var newEdges = TopEdgesAdder.addEdge(statements, newValue);

                    // extract class id of entity from new statements, if there are, or from old, if there are
                    var stmts = newEdges.size() > 0 ? newEdges : aggValue.getEdges();
                    if (stmts.size() > 0) {
                        var firstEdge = newEdges.get(0);
                        aggValue.setClassId(firstEdge.getSourceClassId());

                    }
                    aggValue.setEdges(newEdges);
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
        public final String project_top_outgoing_edges_group_by = "project_top_outgoing_edges_group_by";
        public final String project_top_outgoing_edges_join_project_entity_label = "project_top_outgoing_edges_join_project_entity_label";
        public final String project_top_outgoing_edges_join_community_entity_label = "project_top_outgoing_edges_join_community_entity_label";

    }

}
