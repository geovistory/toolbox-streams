package org.geovistory.toolbox.streams.entity.processors.project;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.geovistory.toolbox.streams.avro.ProjectEntityClassMetadataValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;

public record ProjectEntityClassMetadataReturnValue(StreamsBuilder builder,

                                                    KTable<ProjectEntityKey, ProjectEntityClassMetadataValue> projectEntityClassMetadataTable,
                                                    KStream<ProjectEntityKey, ProjectEntityClassMetadataValue> projectEntityClassMetadataStream) {
}
