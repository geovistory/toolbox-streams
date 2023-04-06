package org.geovistory.toolbox.streams.base.model.processors;

import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.OntomeClassLabelKey;
import org.geovistory.toolbox.streams.avro.OntomeClassLabelValue;

public record OntomeClassLabelReturnValue(KStream<OntomeClassLabelKey, OntomeClassLabelValue> ontomeClassLabelStream) {
}
