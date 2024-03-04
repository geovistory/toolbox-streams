package org.geovistory.toolbox.streams.entity.label.processsors.project;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.ProjectStatementKey;
import org.geovistory.toolbox.streams.avro.ProjectStatementValue;
import org.geovistory.toolbox.streams.avro.StatementEnrichedValue;
import org.geovistory.toolbox.streams.entity.label.AvroSerdes;
import org.geovistory.toolbox.streams.entity.label.OutputTopicNames;
import org.geovistory.toolbox.streams.entity.label.RegisterInnerTopic;
import org.geovistory.toolbox.streams.entity.label.RegisterInputTopic;
import org.geovistory.toolbox.streams.lib.Utils;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;


@ApplicationScoped
public class ProjectStatementWithLiteral {


    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;
    @Inject
    RegisterInnerTopic registerInnerTopic;

    @Inject
    OutputTopicNames outputTopicNames;

    public ProjectStatementWithLiteral(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, RegisterInnerTopic registerInnerTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.registerInnerTopic = registerInnerTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {


        addProcessors(
                registerInputTopic.statementWithLiteralTable(),
                registerInputTopic.proInfoProjRelTable()
        );
    }

    public ProjectStatementReturnValue addProcessors(
            KTable<ts.information.statement.Key, StatementEnrichedValue> enrichedStatementTable,
            KTable<ts.projects.info_proj_rel.Key, ts.projects.info_proj_rel.Value> proInfoProjRelTable) {


        /* STREAM PROCESSORS */
        // 2)
        var projectStatementJoin = proInfoProjRelTable.join(
                enrichedStatementTable,
                value -> ts.information.statement.Key.newBuilder()
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
                Materialized.<ts.projects.info_proj_rel.Key, ProjectStatementValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_statement_with_literal_join)
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

        projectStatementStream.to(outputTopicNames.projectStatementWithLiteral(),
                Produced.with(avroSerdes.ProjectStatementKey(), avroSerdes.ProjectStatementValue())
                        .withName(outputTopicNames.projectStatementWithLiteral() + "-producer")
        );

        return new ProjectStatementReturnValue(projectStatementStream);

    }

    public enum inner {
        TOPICS;
        public final String project_statement_with_literal_join = "project_statement_with_literal_join";

    }

}
