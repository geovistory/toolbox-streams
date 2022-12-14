package org.geovistory.toolbox.streams.topologies;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.OntomeClassLabelKey;
import org.geovistory.toolbox.streams.avro.OntomeClassLabelValue;

public record OntomeClassLabelReturnValue(StreamsBuilder builder,
                                          KStream<OntomeClassLabelKey, OntomeClassLabelValue> ontomeClassLabelStream) {
}
