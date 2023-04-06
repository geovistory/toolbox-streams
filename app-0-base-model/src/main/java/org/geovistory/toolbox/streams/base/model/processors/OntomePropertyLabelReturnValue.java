package org.geovistory.toolbox.streams.base.model.processors;

import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.OntomePropertyLabelKey;
import org.geovistory.toolbox.streams.avro.OntomePropertyLabelValue;

public record OntomePropertyLabelReturnValue(
        KStream<OntomePropertyLabelKey, OntomePropertyLabelValue> ontomePropertyLabelStream) {
}
