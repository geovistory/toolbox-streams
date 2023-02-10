package org.geovistory.toolbox.streams.project.entity.label.processors;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.ProjectStatementKey;
import org.geovistory.toolbox.streams.avro.ProjectStatementValue;
import org.geovistory.toolbox.streams.avro.StatementEnrichedValue;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.project.entity.label.DbTopicNames;
import org.geovistory.toolbox.streams.project.entity.label.Env;
import org.geovistory.toolbox.streams.project.entity.label.RegisterInputTopics;


public class ProjectStatementWithEntity {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerInputTopic = new RegisterInputTopics(builder);

        return addProcessors(
                builder,
                registerInputTopic.statementWithEntityTable(),
                registerInputTopic.proInfoProjRelTable()
        ).builder().build();
    }

    public static ProjectStatementReturnValue addProcessors(
            StreamsBuilder builder,
            KTable<dev.information.statement.Key, StatementEnrichedValue> enrichedStatementWithEntityTable,
            KTable<dev.projects.info_proj_rel.Key, dev.projects.info_proj_rel.Value> proInfoProjRelTable) {

        var avroSerdes = new ConfluentAvroSerdes();


        /* STREAM PROCESSORS */
        // 2)
        var projectStatementJoin = proInfoProjRelTable.join(
                enrichedStatementWithEntityTable,
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
                TableJoined.as(inner.TOPICS.project_statement_with_entity_join + "-fk-join"),
                Materialized.<dev.projects.info_proj_rel.Key, ProjectStatementValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_statement_with_entity_join)
                        .withKeySerde(avroSerdes.ProInfoProjRelKey())
                        .withValueSerde(avroSerdes.ProjectStatementValue())
        );


        var projectStatementStream = projectStatementJoin
                .toStream(
                        Named.as(inner.TOPICS.project_statement_with_entity_join + "-to-stream")
                )
                .selectKey((key, value) -> ProjectStatementKey.newBuilder()
                        .setProjectId(key.getFkProject())
                        .setStatementId(key.getFkEntity())
                        .build(),
                        Named.as("kstream-select-key-project-statement")
                );




        /* SINK PROCESSORS */

        projectStatementStream.to(output.TOPICS.project_statement_with_entity,
                Produced.with(avroSerdes.ProjectStatementKey(), avroSerdes.ProjectStatementValue())
                        .withName(output.TOPICS.project_statement_with_entity + "-producer")
        );

        return new ProjectStatementReturnValue(builder, projectStatementStream);

    }


    public enum input {
        TOPICS;
        public final String pro_info_proj_rel = DbTopicNames.pro_info_proj_rel.getName();
        public final String statement_with_entity = Env.INSTANCE.TOPIC_STATEMENT_WITH_ENTITY;
    }


    public enum inner {
        TOPICS;
        public final String project_statement_with_entity_join = "project_statement_with_entity_join";

    }

    public enum output {
        TOPICS;
        public final String project_statement_with_entity = Utils.tsPrefixed("project_statement_with_entity");
    }

}