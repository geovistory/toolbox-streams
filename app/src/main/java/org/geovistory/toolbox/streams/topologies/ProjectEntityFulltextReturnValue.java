package org.geovistory.toolbox.streams.topologies;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.ProjectEntityFulltextValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;

public record ProjectEntityFulltextReturnValue(StreamsBuilder builder,
                                               KStream<ProjectEntityKey, ProjectEntityFulltextValue> projectEntityFulltextStream) {
}
