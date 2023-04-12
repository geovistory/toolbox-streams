package org.geovistory.toolbox.streams.entity.preview.processors.project;

import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.EntityPreviewValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;

public record ProjectEntityPreviewReturnValue(
        KStream<ProjectEntityKey, EntityPreviewValue> projectEntityPreviewStream) {
}
