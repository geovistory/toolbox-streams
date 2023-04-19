package org.geovistory.toolbox.streams.base.config.processors;

import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.GeovPropertyLabelKey;
import org.geovistory.toolbox.streams.avro.GeovPropertyLabelValue;

public record GeovPropertyLabelReturnValue(
        KStream<GeovPropertyLabelKey, GeovPropertyLabelValue> geovPropertyLabelStream) {
}
