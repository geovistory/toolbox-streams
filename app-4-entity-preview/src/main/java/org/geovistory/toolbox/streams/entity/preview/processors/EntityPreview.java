package org.geovistory.toolbox.streams.entity.preview.processors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.geovistory.toolbox.streams.avro.CommunityEntityKey;
import org.geovistory.toolbox.streams.avro.EntityPreviewValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.entity.preview.ConfiguredAvroSerde;
import org.geovistory.toolbox.streams.entity.preview.OutputTopicNames;
import org.geovistory.toolbox.streams.entity.preview.processors.project.ProjectEntityPreviewReturnValue;

@ApplicationScoped
public class EntityPreview {

    @Inject
    ConfiguredAvroSerde avroSerdes;

    @Inject
    OutputTopicNames outputTopicNames;

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
                Produced.with(avroSerdes.<ProjectEntityKey>key(), avroSerdes.<EntityPreviewValue>value())
                        .withName(outputTopicNames.entityPreview() + "-producer")
        );


        return new ProjectEntityPreviewReturnValue(s);

    }


}
