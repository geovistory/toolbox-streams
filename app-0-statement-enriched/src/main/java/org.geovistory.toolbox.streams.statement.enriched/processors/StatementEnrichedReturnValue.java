package org.geovistory.toolbox.streams.statement.enriched.processors;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.StatementEnrichedValue;

public record StatementEnrichedReturnValue(StreamsBuilder builder,
                                           KStream<dev.information.statement.Key, StatementEnrichedValue> statementEnrichedStream) {
}