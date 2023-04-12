package org.geovistory.toolbox.streams.entity.preview.processors.community;

import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.CommunityEntityKey;
import org.geovistory.toolbox.streams.avro.EntityPreviewValue;

public record CommunityEntityPreviewReturnValue(
        KStream<CommunityEntityKey, EntityPreviewValue> projectEntityPreviewStream) {
}
