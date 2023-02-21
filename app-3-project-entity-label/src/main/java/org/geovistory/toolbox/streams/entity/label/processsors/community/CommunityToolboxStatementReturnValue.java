package org.geovistory.toolbox.streams.entity.label.processsors.community;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.CommunityStatementKey;
import org.geovistory.toolbox.streams.avro.CommunityStatementValue;

public record CommunityToolboxStatementReturnValue(StreamsBuilder builder,
                                                   KStream<CommunityStatementKey, CommunityStatementValue> communityStatementStream) {
}
