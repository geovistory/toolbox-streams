package org.geovistory.toolbox.streams.base.config.processors;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.geovistory.toolbox.streams.avro.CommunityPropertyLabelKey;
import org.geovistory.toolbox.streams.avro.CommunityPropertyLabelValue;

public record CommunityPropertyLabelReturnValue(
        KTable<CommunityPropertyLabelKey, CommunityPropertyLabelValue> communityPropertyLabelTable,
        KStream<CommunityPropertyLabelKey, CommunityPropertyLabelValue> communityPropertyLabelStream
) {
}
