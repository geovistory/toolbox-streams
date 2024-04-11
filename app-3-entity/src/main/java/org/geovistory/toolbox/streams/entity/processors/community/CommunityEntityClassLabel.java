package org.geovistory.toolbox.streams.entity.processors.community;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.entity.I;
import org.geovistory.toolbox.streams.entity.OutputTopicNames;
import org.geovistory.toolbox.streams.entity.RegisterInputTopic;
import org.geovistory.toolbox.streams.entity.processors.project.ProjectEntityClassLabelReturnValue;
import org.geovistory.toolbox.streams.lib.Utils;


@ApplicationScoped
public class CommunityEntityClassLabel {


    @Inject
    ConfiguredAvroSerde avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;


    @Inject
    OutputTopicNames outputTopicNames;

    @ConfigProperty(name = "ts.community.slug", defaultValue = "")
    private String communitySlug;


    public ProjectEntityClassLabelReturnValue addProcessors(
            KTable<ProjectEntityKey, ProjectEntityValue> communityEntityTable,
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
                (value1, value2) -> ProjectEntityClassLabelValue.newBuilder()
                        .setEntityId(value1.getEntityId())
                        .setProjectId(value1.getProjectId())
                        .setClassId(value1.getClassId())
                        .setClassLabel(value2.getLabel())
                        .setDeleted$1(Utils.includesTrue(value1.getDeleted$1(), value2.getDeleted$1()))
                        .build(),
                TableJoined.as(joinName + "-fk-join"),
                Materialized.<ProjectEntityKey, ProjectEntityClassLabelValue, KeyValueStore<Bytes, byte[]>>as(joinName)
                        .withKeySerde(avroSerdes.<ProjectEntityKey>key())
                        .withValueSerde(avroSerdes.<ProjectEntityClassLabelValue>value())
        );


        var communityEntityClassLabelStream = communityEntityClassLabelTable.toStream(
                Named.as(joinName + "-to-stream")
        );
        /* SINK PROCESSORS */

        var outputTopic = outputTopicNames.communityEntityClassLabel();

        communityEntityClassLabelStream.to(outputTopic,
                Produced.with(avroSerdes.<ProjectEntityKey>key(), avroSerdes.<ProjectEntityClassLabelValue>value())
                        .withName(outputTopic + "-producer")
        );

        return new ProjectEntityClassLabelReturnValue(communityEntityClassLabelTable, communityEntityClassLabelStream);

    }



}
