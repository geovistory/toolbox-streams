package org.geovistory.toolbox.streams.entity.label.processsors.community;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.geovistory.toolbox.streams.avro.CommunityStatementKey;
import org.geovistory.toolbox.streams.avro.CommunityStatementValue;
import org.geovistory.toolbox.streams.avro.ProjectStatementKey;
import org.geovistory.toolbox.streams.avro.ProjectStatementValue;
import org.geovistory.toolbox.streams.entity.label.AvroSerdes;

public class CommunityToolboxStatementCounterSupplier extends CommunityStatementCounterSupplier {
    public CommunityToolboxStatementCounterSupplier(String stateStoreName, AvroSerdes avroSerdes) {
        super(stateStoreName, avroSerdes);
    }

    @Override
    public Transformer<ProjectStatementKey, ProjectStatementValue, KeyValue<CommunityStatementKey, CommunityStatementValue>> get() {
        return new CommunityToolboxStatementCounter(stateStoreName);
    }
}
