package org.geovistory.toolbox.streams.topologies;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.ProjectEntityClassLabelValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;

public record ProjectEntityClassLabelLabelReturnValue(StreamsBuilder builder,
                                                      KStream<ProjectEntityKey, ProjectEntityClassLabelValue> projectTopStatementStream) {
}
