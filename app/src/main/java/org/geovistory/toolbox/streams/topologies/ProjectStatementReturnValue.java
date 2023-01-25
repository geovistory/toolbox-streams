package org.geovistory.toolbox.streams.topologies;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.ProjectStatementKey;
import org.geovistory.toolbox.streams.avro.ProjectStatementValue;

public record ProjectStatementReturnValue(StreamsBuilder builder,
                                          KStream<ProjectStatementKey, ProjectStatementValue> ProjectStatementStream) {
}
