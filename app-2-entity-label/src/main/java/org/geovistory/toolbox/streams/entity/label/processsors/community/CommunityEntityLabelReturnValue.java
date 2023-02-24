package org.geovistory.toolbox.streams.entity.label.processsors.community;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.CommunityEntityKey;
import org.geovistory.toolbox.streams.avro.CommunityEntityLabelValue;

public record CommunityEntityLabelReturnValue(StreamsBuilder builder,
                                              KStream<CommunityEntityKey, CommunityEntityLabelValue> projectTopStatementStream) {
}
