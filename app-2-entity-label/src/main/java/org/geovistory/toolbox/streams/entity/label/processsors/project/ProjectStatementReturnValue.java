package org.geovistory.toolbox.streams.entity.label.processsors.project;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.ProjectStatementKey;
import org.geovistory.toolbox.streams.avro.ProjectStatementValue;

public record ProjectStatementReturnValue(StreamsBuilder builder,
                                          KStream<ProjectStatementKey, ProjectStatementValue> ProjectStatementStream) {
}
