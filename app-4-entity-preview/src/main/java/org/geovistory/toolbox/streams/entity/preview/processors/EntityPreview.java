package org.geovistory.toolbox.streams.entity.preview.processors;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.geovistory.toolbox.streams.avro.CommunityEntityKey;
import org.geovistory.toolbox.streams.avro.EntityPreviewValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.entity.preview.RegisterInnerTopic;
import org.geovistory.toolbox.streams.entity.preview.processors.project.ProjectEntityPreviewReturnValue;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;


public class EntityPreview {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder(), "toolbox").describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder, String nameSupplement) {
        var innerTopic = new RegisterInnerTopic(builder);

        return addProcessors(
                builder,
                innerTopic.projectEntityPreviewStream(),
                innerTopic.communityEntityPreviewStream(nameSupplement)
        ).builder().build();
    }

    public static ProjectEntityPreviewReturnValue addProcessors(
            StreamsBuilder builder,
            KStream<ProjectEntityKey, EntityPreviewValue> projectEntityPreviewStream,
            KStream<CommunityEntityKey, EntityPreviewValue> communityEntityPreviewStream
    ) {

        var avroSerdes = new ConfluentAvroSerdes();


        /* STREAM PROCESSORS */
        // 2)

        var s = communityEntityPreviewStream
                .selectKey((key, value) -> ProjectEntityKey.newBuilder()
                        .setEntityId(key.getEntityId()).setProjectId(0).build())
                // 3)
                .merge(projectEntityPreviewStream);
        /* SINK PROCESSORS */

        s.to(output.TOPICS.entity_preview,
                Produced.with(avroSerdes.ProjectEntityKey(), avroSerdes.EntityPreviewValue())
                        .withName(output.TOPICS.entity_preview + "-producer")
        );


        return new ProjectEntityPreviewReturnValue(builder, s);

    }

    public enum output {
        TOPICS;
        public final String entity_preview = Utils.tsPrefixed("entity_preview");
    }

}
