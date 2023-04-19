package org.geovistory.toolbox.streams.entity.label.processsors.community;

import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.CommunityStatementKey;
import org.geovistory.toolbox.streams.avro.CommunityStatementValue;

public record CommunityToolboxStatementReturnValue(
        KStream<CommunityStatementKey, CommunityStatementValue> communityStatementStream) {
}
