package org.geovistory.toolbox.streams.entity.processors.community;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.geovistory.toolbox.streams.avro.ProjectEntityClassLabelValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;

public record CommunityEntityClassLabelReturnValue(
        KTable<ProjectEntityKey, ProjectEntityClassLabelValue> communityEntityClassLabelTable,
        KStream<ProjectEntityKey, ProjectEntityClassLabelValue> communityEntityClassLabelStream) {
}
