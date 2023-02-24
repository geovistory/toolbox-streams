package org.geovistory.toolbox.streams.base.config.processors;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.geovistory.toolbox.streams.avro.ProjectFieldLabelKey;
import org.geovistory.toolbox.streams.avro.ProjectFieldLabelValue;

public record ProjectPropertyLabelReturnValue(StreamsBuilder builder,
                                              KTable<ProjectFieldLabelKey, ProjectFieldLabelValue> projectProfileTable,
                                              KStream<ProjectFieldLabelKey, ProjectFieldLabelValue> projectProfileStream
) {
}
