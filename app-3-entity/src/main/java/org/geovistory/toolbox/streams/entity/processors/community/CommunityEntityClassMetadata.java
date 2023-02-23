package org.geovistory.toolbox.streams.entity.processors.community;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.RegisterInputTopic;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;


public class CommunityEntityClassMetadata {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder(), "toolbox").describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder, String nameSupplement) {
        var inputTopic = new RegisterInputTopic(builder);

        return addProcessors(
                builder,
                inputTopic.communityEntityTable(),
                inputTopic.ontomeClassMetadataTable(),
                nameSupplement
        ).builder().build();
    }

    public static CommunityEntityClassMetadataReturnValue addProcessors(
            StreamsBuilder builder,
            KTable<CommunityEntityKey, CommunityEntityValue> projectEntityTable,
            KTable<OntomeClassKey, OntomeClassMetadataValue> ontomeClassMetadataTable,
            String nameSupplement
    ) {

        var avroSerdes = new ConfluentAvroSerdes();

        var joinName = "communtiy_" + nameSupplement + "_entity_with_class_metadata";

        /* STREAM PROCESSORS */
        // 2)

        var projectEntityClassMetadataTable = projectEntityTable.join(
                ontomeClassMetadataTable,
                projectEntityValue -> OntomeClassKey.newBuilder()
                        .setClassId(projectEntityValue.getClassId())
                        .build(),
                (value1, value2) -> CommunityEntityClassMetadataValue.newBuilder()
                        .setParentClasses(value2.getParentClasses())
                        .setAncestorClasses(value2.getAncestorClasses())
                        .setProjectCount(value1.getProjectCount())
                        .build(),
                TableJoined.as(joinName + "-fk-join"),
                Materialized.<CommunityEntityKey, CommunityEntityClassMetadataValue, KeyValueStore<Bytes, byte[]>>as(joinName)
                        .withKeySerde(avroSerdes.CommunityEntityKey())
                        .withValueSerde(avroSerdes.CommunityEntityClassMetadataValue())
        );


        var projectEntityClassMetadataStream = projectEntityClassMetadataTable.toStream(
                Named.as(joinName + "-to-stream")
        );
        /* SINK PROCESSORS */

        projectEntityClassMetadataStream.to(getOutputTopicName(nameSupplement),
                Produced.with(avroSerdes.CommunityEntityKey(), avroSerdes.CommunityEntityClassMetadataValue())
                        .withName(getOutputTopicName(nameSupplement) + "-producer")
        );

        return new CommunityEntityClassMetadataReturnValue(builder, projectEntityClassMetadataTable, projectEntityClassMetadataStream);

    }


    public static String getOutputTopicName(String nameSupplement) {
        return Utils.tsPrefixed("community_" + nameSupplement + "_entity_class_metadata");
    }

}
