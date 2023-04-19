package org.geovistory.toolbox.streams.entity.label.processsors.project;

import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectEntityLabelValue;

public record ProjectEntityLabelReturnValue(
        KStream<ProjectEntityKey, ProjectEntityLabelValue> projectTopStatementStream) {
}
