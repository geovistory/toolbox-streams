package org.geovistory.toolbox.streams.base.config.processors;

import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.ProjectPropertyKey;
import org.geovistory.toolbox.streams.avro.ProjectPropertyValue;

public record ProjectPropertyReturnValue(
        KStream<ProjectPropertyKey, ProjectPropertyValue> projectPropertyStream) {
}
