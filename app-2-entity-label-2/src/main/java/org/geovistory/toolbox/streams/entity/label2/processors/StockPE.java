package org.geovistory.toolbox.streams.entity.label2.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.EntityValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.entity.label2.stores.PEStore;

public class StockPE implements Processor<ProjectEntityKey, EntityValue, ProjectEntityKey, EntityValue> {
    private KeyValueStore<ProjectEntityKey, EntityValue> peStore;

    @Override
    public void init(ProcessorContext<ProjectEntityKey, EntityValue> context) {
        peStore = context.getStateStore(PEStore.NAME);
    }

    @Override
    public void process(Record<ProjectEntityKey, EntityValue> record) {
        var k = record.key();
        var newV = record.value();

        // stock the project entity
        this.peStore.put(k, newV);
    }
}


