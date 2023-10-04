package org.geovistory.toolbox.streams.entity.processors.community;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.AvroSerdes;
import org.geovistory.toolbox.streams.entity.I;
import org.geovistory.toolbox.streams.entity.OutputTopicNames;
import org.geovistory.toolbox.streams.entity.RegisterInputTopic;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;


@ApplicationScoped
public class CommunityEntityClassLabel {


    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;


    @Inject
    OutputTopicNames outputTopicNames;

    @ConfigProperty(name = "ts.community.slug", defaultValue = "")
    private String communitySlug;


    public CommunityEntityClassLabel(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {
        addProcessors(
                registerInputTopic.communityEntityTable(),
                registerInputTopic.communityClassLabelTable()
        );
    }

    public CommunityEntityClassLabelReturnValue addProcessors(
            KTable<CommunityEntityKey, CommunityEntityValue> communityEntityTable,
            KTable<OntomeClassLabelKey, CommunityClassLabelValue> communityClassLabelTable
    ) {

        /* STREAM PROCESSORS */
        // 2)

        var joinName = "communtiy_" + communitySlug + "_entity_with_class_label";

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

        var outputTopic = outputTopicNames.communityEntityClassLabel();

        communityEntityClassLabelStream.to(outputTopic,
                Produced.with(avroSerdes.CommunityEntityKey(), avroSerdes.CommunityEntityClassLabelValue())
                        .withName(outputTopic + "-producer")
        );

        return new CommunityEntityClassLabelReturnValue( communityEntityClassLabelTable, communityEntityClassLabelStream);

    }



}
