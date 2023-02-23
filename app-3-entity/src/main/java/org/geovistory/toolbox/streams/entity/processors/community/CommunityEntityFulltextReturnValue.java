package org.geovistory.toolbox.streams.entity.processors.community;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.CommunityEntityFulltextValue;
import org.geovistory.toolbox.streams.avro.CommunityEntityKey;

public record CommunityEntityFulltextReturnValue(StreamsBuilder builder,
                                                 KStream<CommunityEntityKey, CommunityEntityFulltextValue> communityEntityFulltextStream) {
}
