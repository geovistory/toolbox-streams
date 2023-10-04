package org.geovistory.toolbox.streams.entity.processors.community;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.AvroSerdes;
import org.geovistory.toolbox.streams.entity.OutputTopicNames;
import org.geovistory.toolbox.streams.entity.RegisterInputTopic;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;


@ApplicationScoped
public class CommunityEntityClassMetadata {


    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;


    @Inject
    OutputTopicNames outputTopicNames;

    @ConfigProperty(name = "ts.community.slug", defaultValue = "")
    private String communitySlug;


    public CommunityEntityClassMetadata(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {
        addProcessors(
                registerInputTopic.communityEntityTable(),
                registerInputTopic.ontomeClassMetadataTable()
        );
    }

    public CommunityEntityClassMetadataReturnValue addProcessors(
            KTable<CommunityEntityKey, CommunityEntityValue> projectEntityTable,
            KTable<OntomeClassKey, OntomeClassMetadataValue> ontomeClassMetadataTable
    ) {


        var joinName = "communtiy_" + communitySlug + "_entity_joins_class_metadata";

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

        projectEntityClassMetadataStream.to(outputTopicNames.communityEntityClassMetadata(),
                Produced.with(avroSerdes.CommunityEntityKey(), avroSerdes.CommunityEntityClassMetadataValue())
                        .withName(outputTopicNames.communityEntityClassMetadata() + "-to-producer")
        );

        return new CommunityEntityClassMetadataReturnValue(projectEntityClassMetadataTable, projectEntityClassMetadataStream);

    }



}
