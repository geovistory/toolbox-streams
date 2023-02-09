package org.geovistory.toolbox.streams.field.changes.processors;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.FieldChangeJoin;
import org.geovistory.toolbox.streams.avro.FieldChangeKey;
import org.geovistory.toolbox.streams.avro.FieldChangeValue;
import org.geovistory.toolbox.streams.field.changes.DbTopicNames;
import org.geovistory.toolbox.streams.field.changes.RegisterInputTopic;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;

import java.time.Instant;


public class ProjectFieldChange {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerInputTopic = new RegisterInputTopic(builder);

        return addProcessors(
                builder,
                registerInputTopic.infStatementTable(),
                registerInputTopic.proInfoProjRelTable()
        ).builder().build();
    }

    public static ProjectFieldChangeReturnValue addProcessors(
            StreamsBuilder builder,
            KTable<dev.information.statement.Key, dev.information.statement.Value> statementTable,
            KTable<dev.projects.info_proj_rel.Key, dev.projects.info_proj_rel.Value> proInfoProjRelTable) {

        var avroSerdes = new ConfluentAvroSerdes();


        /* STREAM PROCESSORS */
        // 2)
        var statementWithDateTable = proInfoProjRelTable.join(
                statementTable,
                value -> dev.information.statement.Key.newBuilder()
                        .setPkEntity(value.getFkEntity())
                        .build(),
                (projectRelation, statement) -> FieldChangeJoin.newBuilder()
                        .setFkProject(projectRelation.getFkProject())
                        .setFkProperty(statement.getFkProperty())
                        .setFkPropertyOfProperty(statement.getFkPropertyOfProperty())
                        .setFkObjectInfo(statement.getFkObjectInfo())
                        .setFkObjectTablesCell(statement.getFkObjectTablesCell())
                        .setFkSubjectInfo(statement.getFkSubjectInfo())
                        .setFkSubjectTablesCell(statement.getFkObjectTablesCell())
                        .setTmspLastModification(Utils.InstantFromIso(projectRelation.getTmspLastModification()))
                        .build(),
                TableJoined.as(inner.TOPICS.project_statement_modification_date_join + "-fk-join"),
                Materialized.<dev.projects.info_proj_rel.Key, FieldChangeJoin, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_statement_modification_date_join)
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
                        .setFkProject(value.getFkProject())
                        .setFkProperty(value.getFkProperty())
                        .setFkPropertyOfProperty(value.getFkPropertyOfProperty())
                        .setFkSourceInfo(value.getFkObjectInfo())
                        .setFkSourceTablesCell(value.getFkObjectTablesCell())
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

        projectFieldChangeStream.to(output.TOPICS.project_field_change,
                Produced.with(avroSerdes.FieldChangeKey(), avroSerdes.FieldChangeValue())
                        .withName(output.TOPICS.project_field_change + "-producer")
        );

        return new ProjectFieldChangeReturnValue(builder, projectFieldChangeStream);

    }


    public enum input {
        TOPICS;
        public final String pro_info_proj_rel = DbTopicNames.pro_info_proj_rel.getName();
        public final String inf_statement = DbTopicNames.inf_statement.getName();
    }


    public enum inner {
        TOPICS;
        public final String project_statement_modification_date_join = "project_statement_modification_date_join";

    }

    public enum output {
        TOPICS;
        public final String project_field_change = Utils.tsPrefixed("project_field_change");
    }

}
