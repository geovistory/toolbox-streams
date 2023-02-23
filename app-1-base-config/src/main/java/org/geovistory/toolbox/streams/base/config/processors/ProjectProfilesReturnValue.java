package org.geovistory.toolbox.streams.base.config.processors;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.ProjectProfileKey;
import org.geovistory.toolbox.streams.avro.ProjectProfileValue;

public record ProjectProfilesReturnValue(StreamsBuilder builder,
                                         KStream<ProjectProfileKey, ProjectProfileValue> projectProfileStream) {
}
