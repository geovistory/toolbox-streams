package org.geovistory.toolbox.streams.analysis.statements.processors;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.ProjectStatementKey;
import org.geovistory.toolbox.streams.avro.ProjectStatementValue;

public record ProjectAnalysisStatementReturnValue(StreamsBuilder builder,
                                                  KStream<ProjectStatementKey, ProjectStatementValue> projectAnalysisStatementStream) {
}
