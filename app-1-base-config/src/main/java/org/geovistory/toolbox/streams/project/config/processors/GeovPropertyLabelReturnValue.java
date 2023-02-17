package org.geovistory.toolbox.streams.project.config.processors;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.GeovPropertyLabelKey;
import org.geovistory.toolbox.streams.avro.GeovPropertyLabelValue;

public record GeovPropertyLabelReturnValue(StreamsBuilder builder,
                                           KStream<GeovPropertyLabelKey, GeovPropertyLabelValue> geovPropertyLabelStream) {
}
