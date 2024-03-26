package org.geovistory.toolbox.streams.project.items.processors;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.EntityValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectStatementKey;
import org.geovistory.toolbox.streams.avro.StatementWithObValue;
import org.geovistory.toolbox.streams.project.items.stores.SObStore;

public class JoinPE_SOb implements Processor<ProjectEntityKey, EntityValue, ProjectStatementKey, StatementWithObValue> {
    private KeyValueStore<String, StatementWithObValue> sObStore;

    private ProcessorContext<ProjectStatementKey, StatementWithObValue> context;

    @Override
    public void init(ProcessorContext<ProjectStatementKey, StatementWithObValue> context) {
        sObStore = context.getStateStore(SObStore.NAME);
        this.context = context;
    }

    @Override
    public void process(Record<ProjectEntityKey, EntityValue> record) {
        var k = record.key();
        var newV = StatementWithObValue.newBuilder()
                .setObjectEntityValue(record.value())
                .build();

        // create prefix in form of {fk_entity}_{fk_project}_
        var prefix = k.getEntityId() + "_" + k.getProjectId() + "_";

        // scan statements with subject store with prefix
        try (var iterator = this.sObStore.prefixScan(prefix, Serdes.String().serializer())) {

            // iterate over all matches in iprStore
            while (iterator.hasNext()) {
                // get key-value record of a statement with subject
                var s = iterator.next();

                // get old joined project entity
                var oldV = s.value;

                // create new join value
                if (oldV != null) newV.setStatementId(oldV.getStatementId());

                // if new differs old
                if (!newV.equals(oldV)) {

                    // update project entity in subject store
                    sObStore.put(s.key, newV);

                    // push downstream
                    if (newV.getStatementId() != null) {
                        var outK = ProjectStatementKey.newBuilder()
                                .setProjectId(k.getProjectId())
                                .setStatementId(newV.getStatementId()).build();
                        this.context.forward(record.withKey(outK).withValue(newV));
                    }
                }
            }
        }
    }
}


