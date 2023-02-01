package org.geovistory.toolbox.streams.topologies;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectEntityTypeValue;

public record ProjectEntityTypeReturnValue(StreamsBuilder builder,
                                           KTable<ProjectEntityKey, ProjectEntityTypeValue> projectEntityTypeTable,
                                           KStream<ProjectEntityKey, ProjectEntityTypeValue> projectEntityTypeStream) {
}
