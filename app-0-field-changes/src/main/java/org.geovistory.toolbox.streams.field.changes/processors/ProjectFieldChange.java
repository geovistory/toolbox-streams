package org.geovistory.toolbox.streams.field.changes.processors;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.FieldChangeJoin;
import org.geovistory.toolbox.streams.avro.FieldChangeKey;
import org.geovistory.toolbox.streams.avro.FieldChangeValue;
import org.geovistory.toolbox.streams.field.changes.AvroSerdes;
import org.geovistory.toolbox.streams.field.changes.RegisterInputTopic;
import org.geovistory.toolbox.streams.lib.TopicNameEnum;
import org.geovistory.toolbox.streams.lib.Utils;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;

@ApplicationScoped
public class ProjectFieldChange {

    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;


    @ConfigProperty(name = "ts.input.topic.name.prefix", defaultValue = "ts")
    String inPrefix;
    @ConfigProperty(name = "ts.output.topic.name.prefix", defaultValue = "")
    public String outPrefix;

    public ProjectFieldChange(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
    }

    public void addProcessorsStandalone() {

        addProcessors(
                registerInputTopic.infStatementTable(),
                registerInputTopic.proInfoProjRelTable()
        );

    }

    public void addProcessors(
            KTable<ts.information.statement.Key, ts.information.statement.Value> statementTable,
            KTable<ts.projects.info_proj_rel.Key, ts.projects.info_proj_rel.Value> proInfoProjRelTable) {

        /* STREAM PROCESSORS */
        // 2)
        var statementWithDateTable = proInfoProjRelTable.join(
                statementTable,
                value -> ts.information.statement.Key.newBuilder()
                        .setPkEntity(value.getFkEntity())
                        .build(),
                (projectRelation, statement) -> FieldChangeJoin.newBuilder()
                        .setFkProject(projectRelation.getFkProject())
                        .setFkProperty(statement.getFkProperty())
                        .setFkPropertyOfProperty(statement.getFkPropertyOfProperty())
                        .setFkObjectInfo(statement.getFkObjectInfo())
                        .setFkObjectTablesCell(statement.getFkObjectTablesCell())
                        .setFkSubjectInfo(statement.getFkSubjectInfo())
                        .setFkSubjectTablesCell(statement.getFkSubjectTablesCell())
                        .setTmspLastModification(
                                // in case record was deleted, the tmsp is null
                                projectRelation.getTmspLastModification() == null ?
                                        // then we "invent" one, that will trigger a field chang update
                                        Instant.now() :
                                        Utils.InstantFromIso(projectRelation.getTmspLastModification())
                        )
                        .build(),
                TableJoined.as(inner.TOPICS.project_statement_modification_date_join + "-fk-join"),
                Materialized.<ts.projects.info_proj_rel.Key, FieldChangeJoin, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_statement_modification_date_join)
                        .withKeySerde(avroSerdes.ProInfoProjRelKey())
                        .withValueSerde(avroSerdes.FieldChangeJoin())
        );


        var statementWithDateTableStream = statementWithDateTable
                .toStream(
                        Named.as(inner.TOPICS.project_statement_modification_date_join + "-to-stream")
                );


        // 3
        var groupedByObject = statementWithDateTableStream.groupBy(
                (key, value) -> FieldChangeKey.newBuilder()
                        .setFkProject(Utils.coalesce(value.getFkProject(), 0))
                        .setFkProperty(Utils.coalesce(value.getFkProperty(), 0))
                        .setFkPropertyOfProperty(Utils.coalesce(value.getFkPropertyOfProperty(), 0))
                        .setFkSourceInfo(Utils.coalesce(value.getFkObjectInfo(), 0))
                        .setFkSourceTablesCell(Utils.coalesce(value.getFkObjectTablesCell(), 0L))
                        .setIsOutgoing(false)
                        .build(),
                Grouped.with(
                        avroSerdes.FieldChangeKey(), avroSerdes.FieldChangeJoin()
                ).withName("kstream-group-by-project-field-change-using-object")
        );
        var aggregatedByObject = groupedByObject.aggregate(
                () -> FieldChangeValue.newBuilder()
                        .setTmspLastModification(Instant.EPOCH)
                        .build(),
                (key, value, aggregate) -> {
                    if (value.getTmspLastModification().isAfter(aggregate.getTmspLastModification())) {
                        aggregate.setTmspLastModification(value.getTmspLastModification());
                    }
                    return aggregate;
                },
                Materialized.<FieldChangeKey, FieldChangeValue, KeyValueStore<Bytes, byte[]>>as("kstream-aggregate-project-field-change-using-object")
                        .withKeySerde(avroSerdes.FieldChangeKey())
                        .withValueSerde(avroSerdes.FieldChangeValue())
        );
        // 4
        var groupedBySubject = statementWithDateTableStream.groupBy(
                (key, value) -> FieldChangeKey.newBuilder()
                        .setFkProject(Utils.coalesce(value.getFkProject(), 0))
                        .setFkProperty(Utils.coalesce(value.getFkProperty(), 0))
                        .setFkPropertyOfProperty(Utils.coalesce(value.getFkPropertyOfProperty(), 0))
                        .setFkSourceInfo(Utils.coalesce(value.getFkSubjectInfo(), 0))
                        .setFkSourceTablesCell(Utils.coalesce(value.getFkSubjectTablesCell(), 0L))
                        .setIsOutgoing(true)
                        .build(),
                Grouped.with(
                        avroSerdes.FieldChangeKey(), avroSerdes.FieldChangeJoin()
                ).withName("kstream-group-by-project-field-change-using-subject")
        );
        var aggregatedBySubject = groupedBySubject.aggregate(
                () -> FieldChangeValue.newBuilder()
                        .setTmspLastModification(Instant.EPOCH)
                        .build(),
                (key, value, aggregate) -> {
                    if (value.getTmspLastModification().isAfter(aggregate.getTmspLastModification())) {
                        aggregate.setTmspLastModification(value.getTmspLastModification());
                    }
                    return aggregate;
                },
                Materialized.<FieldChangeKey, FieldChangeValue, KeyValueStore<Bytes, byte[]>>as("kstream-aggregate-project-field-change-using-subject")
                        .withKeySerde(avroSerdes.FieldChangeKey())
                        .withValueSerde(avroSerdes.FieldChangeValue())
        );
        // 5
        var o = aggregatedByObject.toStream(Named.as("ktable-to-stream-project-field-change-using-object"));
        var s = aggregatedBySubject.toStream(Named.as("ktable-to-stream-project-field-change-using-subject"));

        var projectFieldChangeStream = o.merge(s, Named.as("merge-project-field-changes"));

        /* SINK PROCESSORS */

        projectFieldChangeStream.to(outputTopicProjectFieldChange(),
                Produced.with(avroSerdes.FieldChangeKey(), avroSerdes.FieldChangeValue())
                        .withName(outputTopicProjectFieldChange() + "-producer")
        );


    }


    public String inputTopicProInfoProjRel() {
        return Utils.prefixedIn(inPrefix, TopicNameEnum.pro_info_proj_rel.getValue());
    }

    public String inputTopicInfStatement() {
        return Utils.prefixedIn(inPrefix, TopicNameEnum.inf_statement.getValue());
    }

    public enum inner {
        TOPICS;
        public final String project_statement_modification_date_join = "project_statement_modification_date_join";

    }

    public String outputTopicProjectFieldChange() {
        return Utils.prefixedOut(outPrefix, "project_field_change");
    }

}
