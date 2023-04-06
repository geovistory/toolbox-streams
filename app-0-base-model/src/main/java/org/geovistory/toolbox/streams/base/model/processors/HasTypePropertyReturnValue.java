package org.geovistory.toolbox.streams.base.model.processors;

import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.HasTypePropertyKey;
import org.geovistory.toolbox.streams.avro.HasTypePropertyValue;

public record HasTypePropertyReturnValue(KStream<HasTypePropertyKey, HasTypePropertyValue> hasTypePropertyStream) {
}
