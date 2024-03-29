package org.geovistory.toolbox.streams.project.items.processors;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.IprJoinVal;
import org.geovistory.toolbox.streams.avro.ProjectStatementKey;
import org.geovistory.toolbox.streams.avro.StatementEnrichedValue;
import org.geovistory.toolbox.streams.avro.StatementValue;
import org.geovistory.toolbox.streams.project.items.stores.IprStore;
import org.geovistory.toolbox.streams.project.items.stores.SStore;

import static org.geovistory.toolbox.streams.project.items.lib.Fn.createProjectStatementKey;
import static org.geovistory.toolbox.streams.project.items.lib.Fn.createStatementValue;

public class JoinS_IPR implements Processor<Integer, StatementEnrichedValue, ProjectStatementKey, StatementValue> {
    private KeyValueStore<Integer, StatementEnrichedValue> sStore;
    private KeyValueStore<String, IprJoinVal> iprStore;

    private ProcessorContext<ProjectStatementKey, StatementValue> context;

    @Override
    public void init(ProcessorContext<ProjectStatementKey, StatementValue> context) {
        sStore = context.getStateStore(SStore.NAME);
        iprStore = context.getStateStore(IprStore.NAME);
        this.context = context;
    }

    @Override
    public void process(Record<Integer, StatementEnrichedValue> record) {
        // get new statement value
        var newStatementValue = record.value();

        this.sStore.put(record.key(), newStatementValue);

        // scan iprStore for keys starting with pk_entity
        try (var iterator = this.iprStore.prefixScan(record.key() + "_", Serdes.String().serializer())) {

            // iterate over all matches in iprStore
            while (iterator.hasNext()) {
                // get key-value record
                var iprKV = iterator.next();

                // get old statement value (joined in iprStore)
                var oldStatementValue = iprKV.value.getS();

                // if new differs old
                if (!newStatementValue.equals(oldStatementValue)) {

                    // update iprStore
                    iprKV.value.setS(newStatementValue);
                    iprStore.put(iprKV.key, iprKV.value);

                    // create key value
                    var ipr = iprKV.value.getIpr();
                    ProjectStatementKey k = createProjectStatementKey(ipr);
                    StatementValue v = createStatementValue(newStatementValue, ipr);

                    // push downstream
                    this.context.forward(record.withKey(k).withValue(v));
                }
            }
        }
    }
}


