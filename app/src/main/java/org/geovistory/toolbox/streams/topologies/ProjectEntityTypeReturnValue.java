package org.geovistory.toolbox.streams.topologies;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectEntityTypeValue;

public record ProjectEntityTypeReturnValue(StreamsBuilder builder,
                                           KStream<ProjectEntityKey, ProjectEntityTypeValue> projectTopStatementStream) {
}
