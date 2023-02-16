package org.geovistory.toolbox.streams.base.model.processors;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.geovistory.toolbox.streams.avro.OntomeClassKey;
import org.geovistory.toolbox.streams.avro.OntomeClassMetadataValue;

public record OntomeClassMetadataReturnValue(StreamsBuilder builder,
                                             KStream<OntomeClassKey, OntomeClassMetadataValue> ontomeClassMetadataStream,

                                             KTable<OntomeClassKey, OntomeClassMetadataValue> ontomeClassMetadataTable) {
}
