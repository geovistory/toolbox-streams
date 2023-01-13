package org.geovistory.toolbox.streams.topologies;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectEntityLabelValue;

public record ProjectEntityLabelReturnValue(StreamsBuilder builder,
                                            KTable<ProjectEntityKey, ProjectEntityLabelValue> projectTopStatementTable,
                                            KStream<ProjectEntityKey, ProjectEntityLabelValue> projectTopStatementStream) {
}
