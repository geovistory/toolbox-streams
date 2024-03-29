package org.geovistory.toolbox.streams.entity.processors.project;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.geovistory.toolbox.streams.avro.ProjectEntityClassLabelValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;

public record ProjectEntityClassLabelReturnValue(
        KTable<ProjectEntityKey, ProjectEntityClassLabelValue> projectEntityClassLabelTable,
        KStream<ProjectEntityKey, ProjectEntityClassLabelValue> projectEntityClassLabelStream) {
}
