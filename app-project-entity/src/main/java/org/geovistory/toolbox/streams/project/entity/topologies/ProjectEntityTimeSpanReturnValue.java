package org.geovistory.toolbox.streams.project.entity.topologies;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.TimeSpanValue;

public record ProjectEntityTimeSpanReturnValue(StreamsBuilder builder,
                                               KStream<ProjectEntityKey, TimeSpanValue> projectEntityTimeSpanStream) {
}
