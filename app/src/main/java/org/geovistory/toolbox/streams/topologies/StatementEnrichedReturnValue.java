package org.geovistory.toolbox.streams.topologies;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.geovistory.toolbox.streams.avro.StatementEnrichedValue;

public record StatementEnrichedReturnValue(StreamsBuilder builder,
                                           KTable<dev.information.statement.Key, StatementEnrichedValue> statementEnrichedTable,
                                           KStream<dev.information.statement.Key, StatementEnrichedValue> statementEnrichedStream) {
}
