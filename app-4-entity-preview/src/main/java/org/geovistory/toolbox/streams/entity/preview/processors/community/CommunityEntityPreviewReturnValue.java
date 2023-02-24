package org.geovistory.toolbox.streams.entity.preview.processors.community;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.CommunityEntityKey;
import org.geovistory.toolbox.streams.avro.EntityPreviewValue;

public record CommunityEntityPreviewReturnValue(StreamsBuilder builder,
                                                KStream<CommunityEntityKey, EntityPreviewValue> projectEntityPreviewStream) {
}
