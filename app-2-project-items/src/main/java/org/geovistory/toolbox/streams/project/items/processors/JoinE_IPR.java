package org.geovistory.toolbox.streams.project.items.processors;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.EntityProjectedValue;
import org.geovistory.toolbox.streams.avro.EntityValue;
import org.geovistory.toolbox.streams.avro.IprJoinVal;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.project.items.lib.Fn;
import org.geovistory.toolbox.streams.project.items.stores.EStore;
import org.geovistory.toolbox.streams.project.items.stores.IprStore;
import ts.information.resource.Value;

public class JoinE_IPR implements Processor<Integer, Value, ProjectEntityKey, EntityValue> {
    private KeyValueStore<Integer, EntityProjectedValue> eStore;
    private KeyValueStore<String, IprJoinVal> iprStore;

    private ProcessorContext<ProjectEntityKey, EntityValue> context;

    @Override
    public void init(ProcessorContext<ProjectEntityKey, EntityValue> context) {
        eStore = context.getStateStore(EStore.NAME);
        iprStore = context.getStateStore(IprStore.NAME);
        this.context = context;
    }

    @Override
    public void process(Record<Integer, Value> record) {
        // validate the incoming record
        if (record.value().getFkClass() == null) return;

        // build new entity value
        var newEntityProjectedValue = Fn.createEntityProjectedValue(record.value());

        this.eStore.put(record.key(), newEntityProjectedValue);

        // scan iprStore for keys starting with pk_entity
        try (var iterator = this.iprStore.prefixScan(record.key() + "_", Serdes.String().serializer())) {
            // iterate over all matches in iprStore
            while (iterator.hasNext()) {
                // get key-value record
                var iprKV = iterator.next();

                // get old entity value (joined in iprStore)
                var oldEntityProjectedValue = iprKV.value.getE();

                // if new differs old
                if (!newEntityProjectedValue.equals(oldEntityProjectedValue)) {

                    // update iprStore
                    iprKV.value.setE(newEntityProjectedValue);
                    iprStore.put(iprKV.key, iprKV.value);

                    // validate entity value
                    var ipr = iprKV.value.getIpr();
                    ProjectEntityKey k = Fn.createProjectEntityKey(ipr);
                    var v = Fn.createEntityValue(newEntityProjectedValue, ipr);

                    // push downstream
                    this.context.forward(record.withKey(k).withValue(v));

                }
            }
        }
    }
}


