package org.geovistory.toolbox.streams.project.items.processors;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.EntityValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.StatementWithSubValue;
import org.geovistory.toolbox.streams.project.items.stores.SSubStore;

public class JoinPE_SSub implements Processor<ProjectEntityKey, EntityValue, ProjectEntityKey, StatementWithSubValue> {
    private KeyValueStore<String, StatementWithSubValue> sSubStore;

    private ProcessorContext<ProjectEntityKey, StatementWithSubValue> context;

    @Override
    public void init(ProcessorContext<ProjectEntityKey, StatementWithSubValue> context) {
        sSubStore = context.getStateStore(SSubStore.NAME);
        this.context = context;
    }

    @Override
    public void process(Record<ProjectEntityKey, EntityValue> record) {
        var k = record.key();
        var newV = record.value();


        // create prefix in form of {fk_entity}_{fk_project}_
        var prefix = k.getEntityId() + "_" + k.getProjectId() + "_";

        // scan statements with subject store with prefix
        try (var iterator = this.sSubStore.prefixScan(prefix, Serdes.String().serializer())) {

            // iterate over all matches in iprStore
            while (iterator.hasNext()) {
                // get key-value record of a statement with subject
                var s = iterator.next();

                // get old joined project entity
                var oldV = s.value.getSubjectEntityValue();

                // if new differs old
                if (!newV.equals(oldV)) {

                    // update statements with subject store
                    s.value.setSubjectEntityValue(newV);
                    sSubStore.put(s.key, s.value);


                    // push downstream
                    this.context.forward(record.withKey(k).withValue(s.value));
                }
            }
        }
    }
}


