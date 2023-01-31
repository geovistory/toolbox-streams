package org.geovistory.toolbox.streams.topologies;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.EntityPreviewValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;

public record ProjectEntityPreviewReturnValue(StreamsBuilder builder,
                                              KStream<ProjectEntityKey, EntityPreviewValue> projectEntityPreviewStream) {
}
