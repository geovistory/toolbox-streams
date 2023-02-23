package org.geovistory.toolbox.streams.base.config.processors;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.CommunityEntityLabelConfigKey;
import org.geovistory.toolbox.streams.avro.CommunityEntityLabelConfigValue;

public record CommunityEntityLabelConfigReturnValue(StreamsBuilder builder,
                                                    KStream<CommunityEntityLabelConfigKey, CommunityEntityLabelConfigValue> ProjectStatementStream) {
}
