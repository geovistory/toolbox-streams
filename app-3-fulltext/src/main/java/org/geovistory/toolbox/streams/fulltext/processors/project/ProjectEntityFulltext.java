package org.geovistory.toolbox.streams.fulltext.processors.project;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.fulltext.RegisterInputTopic;
import org.geovistory.toolbox.streams.fulltext.processors.FullTextFactory;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.LinkedList;


public class ProjectEntityFulltext {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var inputTopic = new RegisterInputTopic(builder);

        addProcessors(
                inputTopic.projectEntityWithLabelConfigTable(),
                inputTopic.projectTopStatementsTable(),
                inputTopic.projectPropertyLabelTable()
        );

        return builder.build();
    }

    public static void addProcessors(
            KTable<ProjectEntityKey, ProjectEntityLabelConfigValue> projectEntityWithLabelConfigTable,
            KTable<ProjectTopStatementsKey, ProjectTopStatementsValue> projectTopStatementsTable,
            KTable<ProjectFieldLabelKey, ProjectFieldLabelValue> projectPropertyLabelTable

    ) {

        var avroSerdes = new ConfluentAvroSerdes();


        /* STREAM PROCESSORS */
        // 2
        var projectFieldTopLabelsTable = projectTopStatementsTable.mapValues((readOnlyKey, value) -> {
                    var l = new LinkedList<String>();
                    for (var i : value.getStatements()) {
                        var s = i.getStatement();
                        if (readOnlyKey.getIsOutgoing()) {
                            l.add(s.getObjectLabel());
                        } else {
                            l.add(s.getSubjectLabel());
                        }
                    }
                    var res = ProjectFieldTopLabelsValue.newBuilder()
                            .setTargetLabels(l)
                            .build();

                    if (value.getClassId() != null) {
                        res.setPropertyLabelId(ProjectFieldLabelKey.newBuilder()
                                .setProjectId(value.getProjectId())
                                .setClassId(value.getClassId())
                                .setPropertyId(value.getPropertyId())
                                .setIsOutgoing(value.getIsOutgoing())
                                .build());
                    }

                    return res;
                },
                Materialized.<ProjectTopStatementsKey, ProjectFieldTopLabelsValue, KeyValueStore<Bytes, byte[]>>as("project_field_top_labels_store")
                        .withKeySerde(avroSerdes.ProjectTopStatementsKey())
                        .withValueSerde(avroSerdes.ProjectFieldTopLabelsValue())
        );

        // 3
        var t = projectFieldTopLabelsTable.leftJoin(
                projectPropertyLabelTable,
                ProjectFieldTopLabelsValue::getPropertyLabelId,
                (value1, value2) -> FieldLabelWithTopLabelsValue.newBuilder()
                        .setPropertyId(value2.getPropertyId())
                        .setIsOutgoing(value2.getIsOutgoing())
                        .setPropertyLabel(value2.getLabel())
                        .setTargetLabels(value1.getTargetLabels())
                        .build(),
                TableJoined.as("project_entity_fulltext_join_prop_label" + "-fk-left-join"),
                Materialized.<ProjectTopStatementsKey, FieldLabelWithTopLabelsValue, KeyValueStore<Bytes, byte[]>>as("project_entity_fulltext_join_prop_label")
                        .withKeySerde(avroSerdes.ProjectTopStatementsKey())
                        .withValueSerde(avroSerdes.FieldLabelWithTopLabelsValue())
        );

        // 4

        var grouped = t
                .toStream()
                .groupBy((key, value) -> ProjectEntityKey.newBuilder()
                                .setProjectId(key.getProjectId())
                                .setEntityId(key.getEntityId())
                                .build(),
                        Grouped.with(
                                avroSerdes.ProjectEntityKey(), avroSerdes.FieldLabelWithTopLabelsValue()
                        ).withName("project_fulltext_fields_grouped_by_entity")
                );

        var aggregated = grouped.aggregate(() -> EntityFieldTextMapValue.newBuilder().build(),
                (key, value, aggregate) -> {
                    var map = aggregate.getFields();
                    var k = FullTextFactory.getFieldKey(value.getIsOutgoing(), value.getPropertyId());
                    map.put(k, value);
                    aggregate.setFields(map);
                    return aggregate;
                },
                Materialized.<ProjectEntityKey, EntityFieldTextMapValue, KeyValueStore<Bytes, byte[]>>as("project_fulltext_fields_aggregated_by_entity")
                        .withKeySerde(avroSerdes.ProjectEntityKey())
                        .withValueSerde(avroSerdes.EntityFieldTextMapValue())
        );

        // 5
        var withConfig = aggregated.leftJoin(
                projectEntityWithLabelConfigTable,
                (value1, value2) -> EntityFieldTextMapWithConfigValue.newBuilder()
                        .setFields(value1.getFields())
                        .setLabelConfig(value2 != null && Utils.booleanIsNotEqualTrue(value2.getDeleted$1()) ? value2.getConfig() : null)
                        .build(),
                Named.as("project_entity_fulltext_label_config" + "-fk-left-join"),
                Materialized.<ProjectEntityKey, EntityFieldTextMapWithConfigValue, KeyValueStore<Bytes, byte[]>>as("project_entity_fulltext_label_config")
                        .withKeySerde(avroSerdes.ProjectEntityKey())
                        .withValueSerde(avroSerdes.EntityFieldTextMapWithConfigValue())
        );

        var fulltextTable = withConfig.mapValues((readOnlyKey, value) -> ProjectEntityFulltextValue.newBuilder()
                .setFulltext(FullTextFactory.createFulltext(value))
                .setEntityId(readOnlyKey.getEntityId())
                .setProjectId(readOnlyKey.getProjectId())
                .setDeleted$1(false)
                .build());

        var fulltextStream = fulltextTable.toStream(Named.as("ktable-to-stream-project-fulltext"));

        /* SINK PROCESSORS */

        fulltextStream.to(output.TOPICS.project_entity_fulltext,
                Produced.with(avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityFulltextValue())
                        .withName(output.TOPICS.project_entity_fulltext + "-producer")
        );


    }


    public enum output {
        TOPICS;
        public final String project_entity_fulltext = Utils.tsPrefixed("project_entity_fulltext");
    }

}
