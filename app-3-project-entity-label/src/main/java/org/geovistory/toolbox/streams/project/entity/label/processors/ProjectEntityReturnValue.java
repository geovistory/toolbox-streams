package org.geovistory.toolbox.streams.project.entity.label.processors;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectEntityValue;

public record ProjectEntityReturnValue(StreamsBuilder builder,
                                       KStream<ProjectEntityKey, ProjectEntityValue> projectEntityStream) {
}
