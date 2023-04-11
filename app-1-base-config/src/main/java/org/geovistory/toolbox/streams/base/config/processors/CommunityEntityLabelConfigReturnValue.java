package org.geovistory.toolbox.streams.base.config.processors;

import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.CommunityEntityLabelConfigKey;
import org.geovistory.toolbox.streams.avro.CommunityEntityLabelConfigValue;

public record CommunityEntityLabelConfigReturnValue(
        KStream<CommunityEntityLabelConfigKey, CommunityEntityLabelConfigValue> ProjectStatementStream) {
}
