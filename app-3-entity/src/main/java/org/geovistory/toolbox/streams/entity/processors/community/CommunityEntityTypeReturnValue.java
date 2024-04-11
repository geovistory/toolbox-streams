package org.geovistory.toolbox.streams.entity.processors.community;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectEntityTypeValue;

public record CommunityEntityTypeReturnValue(
        KTable<ProjectEntityKey, ProjectEntityTypeValue> communityEntityTypeTable,
        KStream<ProjectEntityKey, ProjectEntityTypeValue> communityEntityTypeStream) {
}
