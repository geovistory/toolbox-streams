
package org.geovistory.toolbox.streams.entity.preview;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.CommunityEntityKey;
import org.geovistory.toolbox.streams.avro.EntityPreviewValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.entity.preview.processors.community.CommunityEntityPreview;
import org.geovistory.toolbox.streams.entity.preview.processors.project.ProjectEntityPreview;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.InputTopicHelper;

/**
 * This class provides helper methods to register
 * output topics (generated by this app).
 * These helper methods are mainly used for testing.
 */
public class RegisterInnerTopic extends InputTopicHelper {
    public ConfluentAvroSerdes avroSerdes;

    public RegisterInnerTopic(StreamsBuilder builder) {
        super(builder);
        this.avroSerdes = new ConfluentAvroSerdes();
    }


    public KStream<ProjectEntityKey, EntityPreviewValue> projectEntityPreviewStream() {
        return getStream(ProjectEntityPreview.output.TOPICS.project_entity_preview,
                avroSerdes.ProjectEntityKey(), avroSerdes.EntityPreviewValue());
    }

    public KStream<CommunityEntityKey, EntityPreviewValue> communityEntityPreviewStream(String nameSupplement) {
        return getStream(CommunityEntityPreview.getOutputTopicName(nameSupplement),
                avroSerdes.CommunityEntityKey(), avroSerdes.EntityPreviewValue());
    }

}
