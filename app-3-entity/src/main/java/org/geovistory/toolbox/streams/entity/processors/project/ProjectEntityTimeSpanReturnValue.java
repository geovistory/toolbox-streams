package org.geovistory.toolbox.streams.entity.processors.project;

import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.TimeSpanValue;

public record ProjectEntityTimeSpanReturnValue(KStream<ProjectEntityKey, TimeSpanValue> projectEntityTimeSpanStream) {
}
