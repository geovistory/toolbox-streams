package org.geovistory.toolbox.streams.entity.processors.community;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.geovistory.toolbox.streams.avro.CommunityEntityKey;
import org.geovistory.toolbox.streams.avro.CommunityEntityTypeValue;

public record CommunityEntityTypeReturnValue(StreamsBuilder builder,
                                             KTable<CommunityEntityKey, CommunityEntityTypeValue> communityEntityTypeTable,
                                             KStream<CommunityEntityKey, CommunityEntityTypeValue> communityEntityTypeStream) {
}
