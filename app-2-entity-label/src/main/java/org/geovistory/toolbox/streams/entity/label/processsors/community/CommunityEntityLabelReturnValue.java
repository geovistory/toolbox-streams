package org.geovistory.toolbox.streams.entity.label.processsors.community;

import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.CommunityEntityKey;
import org.geovistory.toolbox.streams.avro.CommunityEntityLabelValue;

public record CommunityEntityLabelReturnValue(
        KStream<CommunityEntityKey, CommunityEntityLabelValue> projectTopStatementStream) {
}
