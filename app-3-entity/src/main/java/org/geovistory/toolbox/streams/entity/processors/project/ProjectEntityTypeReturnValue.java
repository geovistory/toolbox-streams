package org.geovistory.toolbox.streams.entity.processors.project;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectEntityTypeValue;

public record ProjectEntityTypeReturnValue(KTable<ProjectEntityKey, ProjectEntityTypeValue> projectEntityTypeTable,
                                           KStream<ProjectEntityKey, ProjectEntityTypeValue> projectEntityTypeStream) {
}
