package org.geovistory.toolbox.streams.topologies;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.app.DbTopicNames;
import org.geovistory.toolbox.streams.app.RegisterInputTopic;
import org.geovistory.toolbox.streams.app.RegisterOutputTopic;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;


public class ProjectStatement {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerInputTopic = new RegisterInputTopic(builder);
        var registerOutputTopic = new RegisterOutputTopic(builder);

        return addProcessors(
                builder,
                registerOutputTopic.statementEnrichedTable(),
                registerInputTopic.proInfoProjRelTable(),
                registerOutputTopic.projectEntityLabelTable()
        ).builder().build();
    }

    public static ProjectStatementReturnValue addProcessors(
            StreamsBuilder builder,
            KTable<dev.information.statement.Key, StatementEnrichedValue> enrichedStatementTable,
            KTable<dev.projects.info_proj_rel.Key, dev.projects.info_proj_rel.Value> proInfoProjRelTable,
            KTable<ProjectEntityKey, ProjectEntityLabelValue> projectEntityLabelTable) {

        var avroSerdes = new ConfluentAvroSerdes();


        /* STREAM PROCESSORS */
        // 2)
        var projectStatementJoin = proInfoProjRelTable.join(
                enrichedStatementTable,
                value -> dev.information.statement.Key.newBuilder()
                        .setPkEntity(value.getFkEntity())
                        .build(),
                (projectRelation, statementEnriched) -> {
                    var v1Deleted = Utils.stringIsEqualTrue(projectRelation.getDeleted$1());
                    var v2Deleted = statementEnriched.getDeleted$1() != null && statementEnriched.getDeleted$1();
                    var notInProject = !projectRelation.getIsInProject();
                    var deleted = v1Deleted || v2Deleted || notInProject;
                    return ProjectStatementValue.newBuilder()
                            .setProjectId(projectRelation.getFkProject())
                            .setStatementId(projectRelation.getFkEntity())
                            .setStatement(statementEnriched)
                            .setOrdNumOfDomain(projectRelation.getOrdNumOfDomain())
                            .setOrdNumOfRange(projectRelation.getOrdNumOfRange())
                            .setCreatedBy(projectRelation.getFkCreator())
                            .setModifiedBy(projectRelation.getFkLastModifier())
                            .setCreatedAt(projectRelation.getTmspCreation())
                            .setModifiedAt(projectRelation.getTmspLastModification())
                            .setDeleted$1(deleted)
                            .build();
                },
                Materialized.<dev.projects.info_proj_rel.Key, ProjectStatementValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_statement_join)
                        .withKeySerde(avroSerdes.ProInfoProjRelKey())
                        .withValueSerde(avroSerdes.ProjectStatementValue())
        );

        // join subject entity labels to get subject label
        var joinSubjectEntityLabel = projectStatementJoin.leftJoin(
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
                Materialized.<dev.projects.info_proj_rel.Key, ProjectStatementValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_statement_join_subject_entity_label)
                        .withKeySerde(avroSerdes.ProInfoProjRelKey())
                        .withValueSerde(avroSerdes.ProjectStatementValue())
        );

        // join object entity labels to get object label
        var projectStatementTable = joinSubjectEntityLabel.leftJoin(projectEntityLabelTable,
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
                Materialized.<dev.projects.info_proj_rel.Key, ProjectStatementValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_statement_join_object_entity_label)
                        .withKeySerde(avroSerdes.ProInfoProjRelKey())
                        .withValueSerde(avroSerdes.ProjectStatementValue()));

        var projectStatementStream = projectStatementTable
                .toStream()
                .map((key, value) -> {
                    var k = ProjectStatementKey.newBuilder()
                            .setProjectId(key.getFkProject())
                            .setStatementId(key.getFkEntity())
                            .build();
                    return KeyValue.pair(k, value);
                });



        /* SINK PROCESSORS */

        projectStatementStream.to(output.TOPICS.project_statement,
                Produced.with(avroSerdes.ProjectStatementKey(), avroSerdes.ProjectStatementValue()));

        return new ProjectStatementReturnValue(builder, projectStatementStream);

    }


    public enum input {
        TOPICS;
        public final String pro_info_proj_rel = DbTopicNames.pro_info_proj_rel.getName();
        public final String statement_enriched = StatementEnriched.output.TOPICS.statement_enriched;
        public final String project_entity_label = ProjectEntityLabel.output.TOPICS.project_entity_label;
    }


    public enum inner {
        TOPICS;
        public final String project_statement_join = "project_statement_join";
        public final String project_statement_join_subject_entity_label = "project_statement_join_subject_entity_label";
        public final String project_statement_join_object_entity_label = "project_statement_join_object_entity_label";
    }

    public enum output {
        TOPICS;
        public final String project_statement = Utils.tsPrefixed("project_statement");
    }

}
