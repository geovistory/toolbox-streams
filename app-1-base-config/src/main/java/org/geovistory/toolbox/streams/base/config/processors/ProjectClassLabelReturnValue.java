package org.geovistory.toolbox.streams.base.config.processors;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.geovistory.toolbox.streams.avro.ProjectClassLabelKey;
import org.geovistory.toolbox.streams.avro.ProjectClassLabelValue;

public record ProjectClassLabelReturnValue(
        KTable<ProjectClassLabelKey, ProjectClassLabelValue> projectClassTable,
        KStream<ProjectClassLabelKey, ProjectClassLabelValue> projectClassStream
) {
}
