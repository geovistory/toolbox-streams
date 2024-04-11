package org.geovistory.toolbox.streams.entity.processors.community;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.entity.OutputTopicNames;
import org.geovistory.toolbox.streams.entity.processors.project.ProjectEntityClassMetadataReturnValue;
import org.geovistory.toolbox.streams.lib.Utils;


@ApplicationScoped
public class CommunityEntityClassMetadata {


    @Inject
    ConfiguredAvroSerde avroSerdes;

    @Inject
    OutputTopicNames outputTopicNames;


    public ProjectEntityClassMetadataReturnValue addProcessors(
            KTable<ProjectEntityKey, ProjectEntityValue> projectEntityTable,
            KTable<OntomeClassKey, OntomeClassMetadataValue> ontomeClassMetadataTable
    ) {


        var joinName = "communtiy_entity_joins_class_metadata";

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
                TableJoined.as(joinName + "-fk-join"),
                Materialized.<ProjectEntityKey, ProjectEntityClassMetadataValue, KeyValueStore<Bytes, byte[]>>as(joinName)
                        .withKeySerde(avroSerdes.key())
                        .withValueSerde(avroSerdes.value())
        );


        var projectEntityClassMetadataStream = projectEntityClassMetadataTable.toStream(
                Named.as(joinName + "-to-stream")
        );
        /* SINK PROCESSORS */

        projectEntityClassMetadataStream.to(outputTopicNames.communityEntityClassMetadata(),
                Produced.with(avroSerdes.<ProjectEntityKey>key(), avroSerdes.<ProjectEntityClassMetadataValue>value())
                        .withName(outputTopicNames.communityEntityClassMetadata() + "-to-producer")
        );

        return new ProjectEntityClassMetadataReturnValue(projectEntityClassMetadataTable, projectEntityClassMetadataStream);

    }



}
