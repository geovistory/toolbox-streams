package org.geovistory.toolbox.streams.entity.label.processsors.project;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.ProjectStatementKey;
import org.geovistory.toolbox.streams.avro.ProjectStatementValue;
import org.geovistory.toolbox.streams.avro.StatementEnrichedValue;
import org.geovistory.toolbox.streams.entity.label.DbTopicNames;
import org.geovistory.toolbox.streams.entity.label.Env;
import org.geovistory.toolbox.streams.entity.label.RegisterInputTopics;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;


public class ProjectStatementWithLiteral {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerInputTopic = new RegisterInputTopics(builder);

        return addProcessors(
                builder,
                registerInputTopic.statementWithLiteralTable(),
                registerInputTopic.proInfoProjRelTable()
        ).builder().build();
    }

    public static ProjectStatementReturnValue addProcessors(
            StreamsBuilder builder,
            KTable<dev.information.statement.Key, StatementEnrichedValue> enrichedStatementTable,
            KTable<dev.projects.info_proj_rel.Key, dev.projects.info_proj_rel.Value> proInfoProjRelTable) {

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
                TableJoined.as(inner.TOPICS.project_statement_with_literal_join + "-fk-join"),
                Materialized.<dev.projects.info_proj_rel.Key, ProjectStatementValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_statement_with_literal_join)
                        .withKeySerde(avroSerdes.ProInfoProjRelKey())
                        .withValueSerde(avroSerdes.ProjectStatementValue())
        );


        var projectStatementStream = projectStatementJoin
                // 3
                .toStream(
                        Named.as(inner.TOPICS.project_statement_with_literal_join + "-to-stream")
                )
                // 4
                .selectKey(
                        (key, value) -> ProjectStatementKey.newBuilder()
                                .setProjectId(key.getFkProject())
                                .setStatementId(key.getFkEntity())
                                .build(),
                        Named.as("kstream-select-key-project-statement-with-literal")
                );


        /* SINK PROCESSORS */

        projectStatementStream.to(output.TOPICS.project_statement_with_literal,
                Produced.with(avroSerdes.ProjectStatementKey(), avroSerdes.ProjectStatementValue())
                        .withName(output.TOPICS.project_statement_with_literal + "-producer")
        );

        return new ProjectStatementReturnValue(builder, projectStatementStream);

    }


    public enum input {
        TOPICS;
        public final String pro_info_proj_rel = DbTopicNames.pro_info_proj_rel.getName();
        public final String statement_with_literal = Env.INSTANCE.TOPIC_STATEMENT_WITH_LITERAL;
    }


    public enum inner {
        TOPICS;
        public final String project_statement_with_literal_join = "project_statement_with_literal_join";

    }

    public enum output {
        TOPICS;
        public final String project_statement_with_literal = Utils.tsPrefixed("project_statement_with_literal");
    }

}
