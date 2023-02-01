package org.geovistory.toolbox.streams.topologies;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.geovistory.toolbox.streams.avro.ProjectClassLabelKey;
import org.geovistory.toolbox.streams.avro.ProjectClassLabelValue;

public record ProjectClassLabelReturnValue(StreamsBuilder builder,
                                           KTable<ProjectClassLabelKey, ProjectClassLabelValue> projectClassTable,
                                           KStream<ProjectClassLabelKey, ProjectClassLabelValue> projectClassStream
) {
}
