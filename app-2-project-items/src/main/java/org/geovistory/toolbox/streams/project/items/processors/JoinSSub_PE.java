package org.geovistory.toolbox.streams.project.items.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.EntityValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.StatementValue;
import org.geovistory.toolbox.streams.avro.StatementWithSubValue;
import org.geovistory.toolbox.streams.project.items.lib.Fn;
import org.geovistory.toolbox.streams.project.items.stores.PEStore;
import org.geovistory.toolbox.streams.project.items.stores.SSubStore;

public class JoinSSub_PE implements Processor<ProjectEntityKey, StatementValue, ProjectEntityKey, StatementWithSubValue> {
    private KeyValueStore<String, StatementWithSubValue> sSubStore;
    private KeyValueStore<ProjectEntityKey, EntityValue> peStore;

    private ProcessorContext<ProjectEntityKey, StatementWithSubValue> context;

    @Override
    public void init(ProcessorContext<ProjectEntityKey, StatementWithSubValue> context) {
        sSubStore = context.getStateStore(SSubStore.NAME);
        peStore = context.getStateStore(PEStore.NAME);
        this.context = context;
    }

    @Override
    public void process(Record<ProjectEntityKey, StatementValue> record) {
        // create key {entity_id}_{project_id}
        var key = SSubStore.createKey(record.key(), record.value());

        // lookup project entity in peStore
        var pe = peStore.get(record.key());

        // lookup old statement with sub
        var oldJoinVal = this.sSubStore.get(key);

        // build new statement with sub
        var newJoinVal = Fn.createStatementWithSubValue(record.value(), pe);

        // if old and new differ
        if (!newJoinVal.equals(oldJoinVal)) {

            // update the sSubStore
            this.sSubStore.put(key, newJoinVal);

            // push downstream
            this.context.forward(record.withValue(newJoinVal));

        }
    }
}


