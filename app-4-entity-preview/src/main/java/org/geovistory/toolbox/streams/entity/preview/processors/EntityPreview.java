package org.geovistory.toolbox.streams.entity.preview.processors;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.geovistory.toolbox.streams.avro.CommunityEntityKey;
import org.geovistory.toolbox.streams.avro.EntityPreviewValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.entity.preview.AvroSerdes;
import org.geovistory.toolbox.streams.entity.preview.OutputTopicNames;
import org.geovistory.toolbox.streams.entity.preview.RegisterInnerTopic;
import org.geovistory.toolbox.streams.entity.preview.processors.project.ProjectEntityPreviewReturnValue;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class EntityPreview {


    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInnerTopic registerInnerTopic;


    @Inject
    OutputTopicNames outputTopicNames;


    public EntityPreview(AvroSerdes avroSerdes, RegisterInnerTopic registerInnerTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInnerTopic = registerInnerTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {

        addProcessors(
                registerInnerTopic.projectEntityPreviewStream(),
                registerInnerTopic.communityEntityPreviewStream()
        );
    }

    public ProjectEntityPreviewReturnValue addProcessors(
            KStream<ProjectEntityKey, EntityPreviewValue> projectEntityPreviewStream,
            KStream<CommunityEntityKey, EntityPreviewValue> communityEntityPreviewStream
    ) {

        /* STREAM PROCESSORS */
        // 2)

        var s = communityEntityPreviewStream
                .selectKey((key, value) -> ProjectEntityKey.newBuilder()
                        .setEntityId(key.getEntityId()).setProjectId(0).build())
                // 3)
                .merge(projectEntityPreviewStream);
        /* SINK PROCESSORS */

        s.to(outputTopicNames.entityPreview(),
                Produced.with(avroSerdes.ProjectEntityKey(), avroSerdes.EntityPreviewValue())
                        .withName(outputTopicNames.entityPreview() + "-producer")
        );


        return new ProjectEntityPreviewReturnValue(s);

    }


}
