package org.geovistory.toolbox.streams.project.entity.topologies;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.project.entity.Env;
import org.geovistory.toolbox.streams.project.entity.RegisterInputTopic;


public class ProjectEntityClassMetadata {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var inputTopic = new RegisterInputTopic(builder);

        return addProcessors(
                builder,
                inputTopic.projectEntityTable(),
                inputTopic.ontomeClassMetadataTable()
        ).builder().build();
    }

    public static ProjectEntityClassMetadataReturnValue addProcessors(
            StreamsBuilder builder,
            KTable<ProjectEntityKey, ProjectEntityValue> projectEntityTable,
            KTable<OntomeClassKey, OntomeClassMetadataValue> ontomeClassMetadataTable
    ) {

        var avroSerdes = new ConfluentAvroSerdes();


        /* STREAM PROCESSORS */
        // 2)

        var projectEntityClassMetadataTable = projectEntityTable.join(
                ontomeClassMetadataTable,
                projectEntityValue -> OntomeClassKey.newBuilder()
                        .setClassId(projectEntityValue.getClassId())
                        .build(),
                (value1, value2) -> ProjectEntityClassMetadataValue.newBuilder()
                        .setParentClasses(value2.getParentClasses())
                        .setAncestorClasses(value2.getAncestorClasses())
                        .setDeleted$1(Utils.booleanIsEqualTrue(value1.getDeleted$1()))
                        .build(),
                TableJoined.as(inner.TOPICS.project_entity_with_class_metadata+ "-fk-join"),
                Materialized.<ProjectEntityKey, ProjectEntityClassMetadataValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_entity_with_class_metadata)
                        .withKeySerde(avroSerdes.ProjectEntityKey())
                        .withValueSerde(avroSerdes.ProjectEntityClassMetadataValue())
        );


        var projectEntityClassMetadataStream = projectEntityClassMetadataTable.toStream(
                Named.as(inner.TOPICS.project_entity_with_class_metadata + "-to-stream")
        );
        /* SINK PROCESSORS */

        projectEntityClassMetadataStream.to(output.TOPICS.project_entity_class_metadata,
                Produced.with(avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityClassMetadataValue())
                        .withName(output.TOPICS.project_entity_class_metadata + "-producer")
        );

        return new ProjectEntityClassMetadataReturnValue(builder, projectEntityClassMetadataTable, projectEntityClassMetadataStream);

    }


    public enum input {
        TOPICS;
        public final String project_entity = Env.INSTANCE.TOPIC_PROJECT_ENTITY;
        public final String ontome_class_metadata = Env.INSTANCE.TOPIC_ONTOME_CLASS_METADATA;
    }


    public enum inner {
        TOPICS;
        public final String project_entity_with_class_metadata = "project_entity_with_class_metadata";

    }

    public enum output {
        TOPICS;
        public final String project_entity_class_metadata = Utils.tsPrefixed("project_entity_class_metadata");
    }


}