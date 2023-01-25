package org.geovistory.toolbox.streams.topologies;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.OntomePropertyLabelKey;
import org.geovistory.toolbox.streams.avro.OntomePropertyLabelValue;

public record OntomePropertyLabelReturnValue(StreamsBuilder builder,
                                             KStream<OntomePropertyLabelKey, OntomePropertyLabelValue> ontomePropertyLabelStream) {
}
