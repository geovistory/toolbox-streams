package org.geovistory.toolbox.streams.project.items.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.EdgeValue;
import org.geovistory.toolbox.streams.avro.ProjectStatementKey;
import org.geovistory.toolbox.streams.avro.StatementJoinValue;
import org.geovistory.toolbox.streams.avro.StatementWithSubValue;
import org.geovistory.toolbox.streams.project.items.lib.Fn;
import org.geovistory.toolbox.streams.project.items.stores.SCompleteStore;

public class JoinSub_Ob implements Processor<ProjectStatementKey, StatementWithSubValue, String, EdgeValue> {
    private KeyValueStore<ProjectStatementKey, StatementJoinValue> sCompleteStore;

    private ProcessorContext<String, EdgeValue> context;

    @Override
    public void init(ProcessorContext<String, EdgeValue> context) {
        sCompleteStore = context.getStateStore(SCompleteStore.NAME);
        this.context = context;
    }

    @Override
    public void process(Record<ProjectStatementKey, StatementWithSubValue> record) {
        var k = record.key();
        var newV = record.value();

        // lookup old complete statement
        var oldJoinVal = this.sCompleteStore.get(k);
        var oldObjectPE = oldJoinVal != null ? oldJoinVal.getObjectEntityValue() : null;
        // build new statement with sub
        var newJoinVal = Fn.createStatementJoinValue(newV, oldObjectPE);

        // if old and new differ
        if (!newJoinVal.equals(oldJoinVal)) {

            // update the sCompleteStore
            this.sCompleteStore.put(k, newJoinVal);

            try {
                var edgeOutV = Fn.createOutgoingEdge(newJoinVal);
                var edgeOutK = Fn.createEdgeKey(edgeOutV);
                // push downstream
                this.context.forward(record.withKey(edgeOutK).withValue(edgeOutV));
            } catch (Exception ignored) {
            }

            try {
                var edgeInV = Fn.createIncomingEdge(newJoinVal);
                var edgeInK = Fn.createEdgeKey(edgeInV);
                // push downstream
                this.context.forward(record.withKey(edgeInK).withValue(edgeInV));
            } catch (Exception ignored) {
            }
        }
    }
}


