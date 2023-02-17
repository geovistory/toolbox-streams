package org.geovistory.toolbox.streams.project.config.processors;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.geovistory.toolbox.streams.avro.CommunityClassLabelValue;
import org.geovistory.toolbox.streams.avro.OntomeClassLabelKey;

public record CommunityClassLabelReturnValue(StreamsBuilder builder,
                                             KTable<OntomeClassLabelKey, CommunityClassLabelValue> communityClassLabelTable,
                                             KStream<OntomeClassLabelKey, CommunityClassLabelValue> communityClassLabelStream
) {
}
