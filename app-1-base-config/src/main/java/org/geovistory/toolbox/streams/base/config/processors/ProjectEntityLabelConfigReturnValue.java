package org.geovistory.toolbox.streams.base.config.processors;

import org.apache.kafka.streams.kstream.KTable;
import org.geovistory.toolbox.streams.avro.ProjectClassKey;
import org.geovistory.toolbox.streams.avro.ProjectEntityLabelConfigValue;

public record ProjectEntityLabelConfigReturnValue(
        KTable<ProjectClassKey, ProjectEntityLabelConfigValue> ProjectStatementStream) {
}
