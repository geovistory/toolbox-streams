package org.geovistory.toolbox.streams.entity.label.processsors.base;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectEntityVisibilityValue;

public record ProjectEntityVisibilityReturnValue(StreamsBuilder builder,
                                                 KStream<ProjectEntityKey, ProjectEntityVisibilityValue> projectEntityVisibilityStream) {
}
