package org.geovistory.toolbox.streams.base.config.processors;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.geovistory.toolbox.streams.avro.ProjectClassKey;
import org.geovistory.toolbox.streams.avro.ProjectEntityLabelConfigValue;

public record ProjectEntityLabelConfigReturnValue(StreamsBuilder builder,
                                                  KTable<ProjectClassKey, ProjectEntityLabelConfigValue> ProjectStatementStream) {
}
