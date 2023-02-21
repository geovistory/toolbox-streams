package org.geovistory.toolbox.streams.project.config.processors;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.ProjectPropertyKey;
import org.geovistory.toolbox.streams.avro.ProjectPropertyValue;

public record ProjectPropertyReturnValue(StreamsBuilder builder,
                                         KStream<ProjectPropertyKey, ProjectPropertyValue> projectPropertyStream) {
}
