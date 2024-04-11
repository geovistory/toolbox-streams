package org.geovistory.toolbox.streams.entity.processors.project;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.entity.OutputTopicNames;
import org.geovistory.toolbox.streams.lib.Utils;


@ApplicationScoped
public class ProjectEntityClassMetadata {

    @Inject
    ConfiguredAvroSerde avroSerdes;

    @Inject
    OutputTopicNames outputTopicNames;

    public ProjectEntityClassMetadataReturnValue addProcessors(
            KTable<ProjectEntityKey, ProjectEntityValue> projectEntityTable,
            KTable<OntomeClassKey, OntomeClassMetadataValue> ontomeClassMetadataTable
    ) {


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
                TableJoined.as(inner.TOPICS.project_entity_with_class_metadata + "-fk-join"),
                Materialized.<ProjectEntityKey, ProjectEntityClassMetadataValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.project_entity_with_class_metadata)
                        .withKeySerde(avroSerdes.key())
                        .withValueSerde(avroSerdes.value())
        );


        var projectEntityClassMetadataStream = projectEntityClassMetadataTable.toStream(
                Named.as(inner.TOPICS.project_entity_with_class_metadata + "-to-stream")
        );
        /* SINK PROCESSORS */

        projectEntityClassMetadataStream.to(outputTopicNames.projectEntityClassMetadata(),
                Produced.with(avroSerdes.<ProjectEntityKey>key(), avroSerdes.<ProjectEntityClassMetadataValue>value())
                        .withName(outputTopicNames.projectEntityClassMetadata() + "-producer")
        );

        return new ProjectEntityClassMetadataReturnValue(projectEntityClassMetadataTable, projectEntityClassMetadataStream);

    }


    public enum inner {
        TOPICS;
        public final String project_entity_with_class_metadata = "project_entity_with_class_metadata";

    }


}
