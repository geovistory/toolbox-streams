package org.geovistory.toolbox.streams.project.entity.preview.processors;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.project.entity.preview.Env;
import org.geovistory.toolbox.streams.project.entity.preview.Klass;
import org.geovistory.toolbox.streams.project.entity.preview.RegisterInputTopic;


public class ProjectEntityPreview {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var inputTopic = new RegisterInputTopic(builder);

        return addProcessors(
                builder,
                inputTopic.projectEntityTable(),
                inputTopic.projectEntityLabelTable(),
                inputTopic.projectEntityClassLabelTable(),
                inputTopic.projectEntityTypeTable(),
                inputTopic.projectEntityTimeSpanTable(),
                inputTopic.projectEntityFulltextTable(),
                inputTopic.projectEntityClassMetadataTable()

        ).builder().build();
    }

    public static ProjectEntityPreviewReturnValue addProcessors(
            StreamsBuilder builder,
            KTable<ProjectEntityKey, ProjectEntityValue> projectEntityTable,
            KTable<ProjectEntityKey, ProjectEntityLabelValue> projectEntityLabelTable,
            KTable<ProjectEntityKey, ProjectEntityClassLabelValue> projectEntityClassLabelTable,
            KTable<ProjectEntityKey, ProjectEntityTypeValue> projectEntityTypeTable,
            KTable<ProjectEntityKey, TimeSpanValue> projectEntityTimeSpanTable,
            KTable<ProjectEntityKey, ProjectEntityFulltextValue> projectEntityFulltextTable,
            KTable<ProjectEntityKey, ProjectEntityClassMetadataValue> projectEntityClassMetadataTable
    ) {

        var avroSerdes = new ConfluentAvroSerdes();


        /* STREAM PROCESSORS */
        // 2)

        var labelJoined = projectEntityTable.leftJoin(
                projectEntityLabelTable,
                (value1, value2) -> {
                    if (Utils.booleanIsEqualTrue(value1.getDeleted$1())) return null;
                    var newVal = EntityPreviewValue.newBuilder()
                            .setFkProject(value1.getProjectId())
                            .setProject(value1.getProjectId())
                            .setEntityId(value1.getEntityId())
                            .setPkEntity(parseStringId(value1.getEntityId()))
                            .setFkClass(value1.getClassId())
                            .setParentClasses("[]")
                            .setAncestorClasses("[]")
                            .setEntityType("")
                            .build();

                    if (value2 != null) newVal.setEntityLabel(value2.getLabel());

                    return newVal;
                },
                Named.as(inner.TOPICS.project_entity_preview_label_join+ "-left-join"),
                Materialized.<ProjectEntityKey, EntityPreviewValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_entity_preview_label_join)
                        .withKeySerde(avroSerdes.ProjectEntityKey())
                        .withValueSerde(avroSerdes.EntityPreviewValue())
        );

        // 3
        var classLabelJoin = labelJoined.leftJoin(
                projectEntityClassLabelTable,
                (value1, value2) -> {
                    if (value2 != null) {
                        value1.setClassLabel(value2.getClassLabel());
                    }
                    return value1;
                },
                Named.as(inner.TOPICS.project_entity_preview_class_label_join+ "-left-join"),
                Materialized.<ProjectEntityKey, EntityPreviewValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_entity_preview_class_label_join)
                        .withKeySerde(avroSerdes.ProjectEntityKey())
                        .withValueSerde(avroSerdes.EntityPreviewValue())
        );
        // 4
        var typeJoined = classLabelJoin.leftJoin(
                projectEntityTypeTable,
                (value1, value2) -> {
                    if (value2 != null) {
                        var typeId = value2.getTypeId();
                        if (typeId != null && typeId.length() > 1) {
                            value1.setTypeId(typeId);
                            value1.setFkType(parseStringId(typeId));
                        }
                        value1.setTypeLabel(value2.getTypeLabel());
                    }
                    return value1;
                },
                Named.as(inner.TOPICS.project_entity_preview_type_join+ "-left-join"),
                Materialized.<ProjectEntityKey, EntityPreviewValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_entity_preview_type_join)
                        .withKeySerde(avroSerdes.ProjectEntityKey())
                        .withValueSerde(avroSerdes.EntityPreviewValue())
        );
        // 5
        var typeTimeSpan = typeJoined.leftJoin(
                projectEntityTimeSpanTable,
                (value1, value2) -> {
                    if (value2 != null) {
                        value1.setTimeSpan(value2.getTimeSpan().toString());
                        value1.setFirstSecond(value2.getFirstSecond());
                        value1.setLastSecond(value2.getLastSecond());
                    }
                    return value1;
                },
                Named.as(inner.TOPICS.project_entity_preview_time_span_join+ "-left-join"),
                Materialized.<ProjectEntityKey, EntityPreviewValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_entity_preview_time_span_join)
                        .withKeySerde(avroSerdes.ProjectEntityKey())
                        .withValueSerde(avroSerdes.EntityPreviewValue())
        );
        // 6
        var typeFulltext = typeTimeSpan.leftJoin(
                projectEntityFulltextTable,
                (value1, value2) -> {
                    if (value2 != null) {
                        value1.setFullText(value2.getFulltext());
                    }
                    return value1;
                },
                Named.as(inner.TOPICS.project_entity_preview_fulltext_join+ "-left-join"),
                Materialized.<ProjectEntityKey, EntityPreviewValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_entity_preview_fulltext_join)
                        .withKeySerde(avroSerdes.ProjectEntityKey())
                        .withValueSerde(avroSerdes.EntityPreviewValue())
        );

        // 7
        var classMetadata = typeFulltext.leftJoin(
                projectEntityClassMetadataTable,
                (value1, value2) -> {
                    if (value2 != null) {
                        var parents = value2.getParentClasses();
                        var ancestors = value2.getAncestorClasses();
                        value1.setParentClasses(parents.toString());
                        value1.setAncestorClasses(ancestors.toString());
                        var isPersistentItem = parents.contains(Klass.PERSISTENT_ITEM.get()) ||
                                ancestors.contains(Klass.PERSISTENT_ITEM.get());
                        var entityType = isPersistentItem ? "peIt" : "teEn";
                        value1.setEntityType(entityType);
                    }
                    return value1;
                },
                Named.as(inner.TOPICS.project_entity_class_metadata_join+ "-left-join"),
                Materialized.<ProjectEntityKey, EntityPreviewValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_entity_class_metadata_join)
                        .withKeySerde(avroSerdes.ProjectEntityKey())
                        .withValueSerde(avroSerdes.EntityPreviewValue())
        );

        var projectEntityPreviewStream = classMetadata.toStream(
                Named.as(inner.TOPICS.project_entity_class_metadata_join + "-to-stream")
        );

        /* SINK PROCESSORS */

        projectEntityPreviewStream.to(output.TOPICS.project_entity_preview,
                Produced.with(avroSerdes.ProjectEntityKey(), avroSerdes.EntityPreviewValue())
                        .withName(output.TOPICS.project_entity_preview + "-producer")
        );

        return new ProjectEntityPreviewReturnValue(builder, projectEntityPreviewStream);

    }

    private static int parseStringId(String value1) {
        try {
            return Integer.parseInt(value1.substring(1));
        } catch (NumberFormatException | IndexOutOfBoundsException e) {
            e.printStackTrace();
        }
        return 0;
    }


    public enum input {
        TOPICS;
        public final String project_entity = Env.INSTANCE.TOPIC_PROJECT_ENTITY;
        public final String project_entity_label =  Env.INSTANCE.TOPIC_PROJECT_ENTITY_LABEL;
        public final String project_entity_class_label =  Env.INSTANCE.TOPIC_PROJECT_ENTITY_CLASS_LABEL;
        public final String project_entity_type =  Env.INSTANCE.TOPIC_PROJECT_ENTITY_TYPE;
        public final String project_entity_time_span =  Env.INSTANCE.TOPIC_PROJECT_ENTITY_TIME_SPAN;
        public final String project_entity_fulltext =  Env.INSTANCE.TOPIC_PROJECT_ENTITY_FULLTEXT;
        public final String project_entity_class_metadata =  Env.INSTANCE.TOPIC_PROJECT_ENTITY_CLASS_METADATA;

    }


    public enum inner {
        TOPICS;
        public final String project_entity_preview_label_join = "project_entity_preview_label_join";
        public final String project_entity_preview_class_label_join = "project_entity_preview_class_label_join";
        public final String project_entity_preview_type_join = "project_entity_preview_type_join";
        public final String project_entity_preview_time_span_join = "project_entity_preview_time_span_join";
        public final String project_entity_preview_fulltext_join = "project_entity_preview_fulltext_join";
        public final String project_entity_class_metadata_join = "project_entity_class_metadata_join";
    }

    public enum output {
        TOPICS;
        public final String project_entity_preview = Utils.tsPrefixed("project_entity_preview");
    }

}