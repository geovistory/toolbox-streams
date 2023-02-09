package org.geovistory.toolbox.streams.base.model.processors;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.HasTypePropertyKey;
import org.geovistory.toolbox.streams.avro.HasTypePropertyValue;

public record HasTypePropertyReturnValue(StreamsBuilder builder,
                                         KStream<HasTypePropertyKey, HasTypePropertyValue> hasTypePropertyStream) {
}
