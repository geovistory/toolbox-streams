package org.geovistory.toolbox.streams.entity.processors.community;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.CommunityEntityKey;
import org.geovistory.toolbox.streams.avro.TimeSpanValue;

public record CommunityEntityTimeSpanReturnValue(StreamsBuilder builder,
                                                 KStream<CommunityEntityKey, TimeSpanValue> communityEntityTimeSpanStream) {
}
