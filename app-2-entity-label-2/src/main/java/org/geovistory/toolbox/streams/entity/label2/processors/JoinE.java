package org.geovistory.toolbox.streams.entity.label2.processors;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.EntityValue;
import org.geovistory.toolbox.streams.avro.IprJoinVal;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectEntityVisibilitiesValue;
import org.geovistory.toolbox.streams.entity.label2.stores.EStore;
import org.geovistory.toolbox.streams.entity.label2.stores.IprStore;
import ts.information.resource.Key;
import ts.information.resource.Value;

import static org.geovistory.toolbox.streams.entity.label2.lib.Fn.*;

public class JoinE implements Processor<Key, Value, ProjectEntityKey, ProjectEntityVisibilitiesValue> {
    private KeyValueStore<Key, EntityValue> eStore;
    private KeyValueStore<String, IprJoinVal> iprStore;

    private ProcessorContext<ProjectEntityKey, ProjectEntityVisibilitiesValue> context;

    @Override
    public void init(ProcessorContext<ProjectEntityKey, ProjectEntityVisibilitiesValue> context) {
        eStore = context.getStateStore(EStore.NAME);
        iprStore = context.getStateStore(IprStore.NAME);
        this.context = context;
    }

    @Override
    public void process(Record<Key, Value> record) {
        // validate the incoming record
        if (record.value().getFkClass() == null) return;

        // build new entity value
        var newEntityValue = createEntityValue(record.value());

        this.eStore.put(record.key(), newEntityValue);

        // scan iprStore for keys starting with pk_entity
        var iterator = this.iprStore.prefixScan(record.key().getPkEntity() + "_", Serdes.String().serializer());

        // iterate over all matches in iprStore
        while (iterator.hasNext()) {
            // get key-value record
            var iprKV = iterator.next();

            // get old entity value (joined in iprStore)
            var oldEntityValue = iprKV.value.getE();

            // if new differs old
            if (!newEntityValue.equals(oldEntityValue)) {

                // update iprStore
                iprKV.value.setE(newEntityValue);
                iprStore.put(iprKV.key, iprKV.value);

                // validate entity value
                var ipr = iprKV.value.getIpr();
                ProjectEntityKey k = createProjectEntityKey(ipr);
                var v = createProjectEntityValue(newEntityValue, ipr);

                // push downstream
                this.context.forward(record.withKey(k).withValue(v));

            }
        }
    }
}


