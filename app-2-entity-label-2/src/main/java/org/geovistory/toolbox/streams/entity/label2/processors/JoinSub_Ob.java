package org.geovistory.toolbox.streams.entity.label2.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.label2.stores.ObByStmtStore;
import org.geovistory.toolbox.streams.entity.label2.stores.SCompleteStore;

import static org.geovistory.toolbox.streams.entity.label2.lib.Fn.*;

public class JoinSub_Ob implements Processor<ProjectStatementKey, StatementWithSubValue, String, EdgeValue> {
    private KeyValueStore<ProjectStatementKey, StatementJoinValue> sCompleteStore;
    private KeyValueStore<ProjectStatementKey, EntityValue> obByStmtStore;

    private ProcessorContext<String, EdgeValue> context;

    @Override
    public void init(ProcessorContext<String, EdgeValue> context) {
        sCompleteStore = context.getStateStore(SCompleteStore.NAME);
        obByStmtStore = context.getStateStore(ObByStmtStore.NAME);
        this.context = context;
    }

    @Override
    public void process(Record<ProjectStatementKey, StatementWithSubValue> record) {
        var k = record.key();
        var newV = record.value();

        // lookup old statement with sub
        var oldJoinVal = this.sCompleteStore.get(k);

        // lookup object
        var objectEntity = this.obByStmtStore.get(k);

        // build new statement with sub
        var newJoinVal = createStatementJoinValue(newV, objectEntity);

        // if old and new differ
        if (!newJoinVal.equals(oldJoinVal)) {

            // update the sCompleteStore
            this.sCompleteStore.put(k, newJoinVal);

            var edgeOutV = createOutgoingEdge(newJoinVal);
            var edgeOutK = createEdgeKey(edgeOutV);

            // push downstream
            this.context.forward(record.withKey(edgeOutK).withValue(edgeOutV));

        }
    }
}


