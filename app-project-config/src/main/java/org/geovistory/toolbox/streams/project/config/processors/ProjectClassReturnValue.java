package org.geovistory.toolbox.streams.project.config.processors;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.ProjectClassKey;
import org.geovistory.toolbox.streams.avro.ProjectClassValue;

public record ProjectClassReturnValue(StreamsBuilder builder,
                                      KStream<ProjectClassKey, ProjectClassValue> projectClassStream) {
}
