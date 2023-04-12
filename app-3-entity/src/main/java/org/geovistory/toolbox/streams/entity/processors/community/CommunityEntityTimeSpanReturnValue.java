package org.geovistory.toolbox.streams.entity.processors.community;

import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.CommunityEntityKey;
import org.geovistory.toolbox.streams.avro.TimeSpanValue;

public record CommunityEntityTimeSpanReturnValue(
        KStream<CommunityEntityKey, TimeSpanValue> communityEntityTimeSpanStream) {
}
