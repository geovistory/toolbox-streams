package org.geovistory.toolbox.streams.field.changes.processors;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.FieldChangeKey;
import org.geovistory.toolbox.streams.avro.FieldChangeValue;

public record ProjectFieldChangeReturnValue(StreamsBuilder builder,
                                            KStream<FieldChangeKey, FieldChangeValue> projectFieldChangeStream) {
}
