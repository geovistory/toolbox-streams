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
import org.geovistory.toolbox.streams.avro.ProjectStatementKey;
import org.geovistory.toolbox.streams.avro.ProjectStatementValue;
import org.geovistory.toolbox.streams.avro.StatementEnrichedValue;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;


public class ProjectStatementWithLiteral {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerInputTopic = new RegisterInputTopic(builder);
        var registerOutputTopic = new RegisterOutputTopic(builder);

        return addProcessors(
                builder,
                registerOutputTopic.statementWithLiteralTable(),
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
                Materialized.<dev.projects.info_proj_rel.Key, ProjectStatementValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_statement_with_literal_join)
                        .withKeySerde(avroSerdes.ProInfoProjRelKey())
                        .withValueSerde(avroSerdes.ProjectStatementValue())
        );


        var projectStatementStream = projectStatementJoin
                .toStream()
                .map((key, value) -> {
                    var k = ProjectStatementKey.newBuilder()
                            .setProjectId(key.getFkProject())
                            .setStatementId(key.getFkEntity())
                            .build();
                    return KeyValue.pair(k, value);
                });



        /* SINK PROCESSORS */

        projectStatementStream.to(output.TOPICS.project_statement_with_literal,
                Produced.with(avroSerdes.ProjectStatementKey(), avroSerdes.ProjectStatementValue()));

        return new ProjectStatementReturnValue(builder, projectStatementStream);

    }


    public enum input {
        TOPICS;
        public final String pro_info_proj_rel = DbTopicNames.pro_info_proj_rel.getName();
        public final String statement_with_literal = StatementEnriched.output.TOPICS.statement_with_literal;
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
