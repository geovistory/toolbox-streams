package org.geovistory.toolbox.streams.entity.processors.community;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.I;
import org.geovistory.toolbox.streams.entity.RegisterInputTopic;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;


public class CommunityEntityClassLabel {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder(), "toolbox").describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder, String nameSupplement) {
        var inputTopic = new RegisterInputTopic(builder);

        return addProcessors(
                builder,
                inputTopic.communityEntityTable(),
                inputTopic.communityClassLabelTable(),
                nameSupplement
        ).builder().build();
    }

    public static CommunityEntityClassLabelReturnValue addProcessors(
            StreamsBuilder builder,
            KTable<CommunityEntityKey, CommunityEntityValue> communityEntityTable,
            KTable<OntomeClassLabelKey, CommunityClassLabelValue> communityClassLabelTable,
            String nameSupplement
    ) {

        var avroSerdes = new ConfluentAvroSerdes();


        /* STREAM PROCESSORS */
        // 2)

        var joinName = "communtiy_" + nameSupplement + "_entity_with_class_label";

        var communityEntityClassLabelTable = communityEntityTable.join(
                communityClassLabelTable,
                communityEntityValue -> OntomeClassLabelKey.newBuilder()
                        .setClassId(communityEntityValue.getClassId())
                        .setLanguageId(I.EN.get())
                        .build(),
                (value1, value2) -> CommunityEntityClassLabelValue.newBuilder()
                        .setClassId(value1.getClassId())
                        .setClassLabel(value2.getLabel())
                        .setProjectCount(value1.getProjectCount())
                        .build(),
                TableJoined.as(joinName + "-fk-join"),
                Materialized.<CommunityEntityKey, CommunityEntityClassLabelValue, KeyValueStore<Bytes, byte[]>>as(joinName)
                        .withKeySerde(avroSerdes.CommunityEntityKey())
                        .withValueSerde(avroSerdes.CommunityEntityClassLabelValue())
        );


        var communityEntityClassLabelStream = communityEntityClassLabelTable.toStream(
                Named.as(joinName + "-to-stream")
        );
        /* SINK PROCESSORS */

        var outputTopic = getOutputTopicName(nameSupplement);

        communityEntityClassLabelStream.to(outputTopic,
                Produced.with(avroSerdes.CommunityEntityKey(), avroSerdes.CommunityEntityClassLabelValue())
                        .withName(outputTopic + "-producer")
        );

        return new CommunityEntityClassLabelReturnValue(builder, communityEntityClassLabelTable, communityEntityClassLabelStream);

    }


    public static String getOutputTopicName(String nameSupplement) {
        return Utils.tsPrefixed("community_" + nameSupplement + "_entity_class_label");
    }
}
