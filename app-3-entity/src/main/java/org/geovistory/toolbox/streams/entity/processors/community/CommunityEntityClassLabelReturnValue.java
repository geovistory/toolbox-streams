package org.geovistory.toolbox.streams.entity.processors.community;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.geovistory.toolbox.streams.avro.CommunityEntityClassLabelValue;
import org.geovistory.toolbox.streams.avro.CommunityEntityKey;

public record CommunityEntityClassLabelReturnValue(StreamsBuilder builder,

                                                   KTable<CommunityEntityKey, CommunityEntityClassLabelValue> communityEntityClassLabelTable,
                                                   KStream<CommunityEntityKey, CommunityEntityClassLabelValue> communityEntityClassLabelStream) {
}
