package org.geovistory.toolbox.streams.base.config.processors;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.GeovClassLabelKey;
import org.geovistory.toolbox.streams.avro.GeovClassLabelValue;

public record GeovClassLabelReturnValue(StreamsBuilder builder,
                                        KStream<GeovClassLabelKey, GeovClassLabelValue> geovClassLabelStream) {
}
