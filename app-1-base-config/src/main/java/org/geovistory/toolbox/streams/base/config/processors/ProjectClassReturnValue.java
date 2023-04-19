package org.geovistory.toolbox.streams.base.config.processors;

import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.ProjectClassKey;
import org.geovistory.toolbox.streams.avro.ProjectClassValue;

public record ProjectClassReturnValue(
                                      KStream<ProjectClassKey, ProjectClassValue> projectClassStream) {
}
