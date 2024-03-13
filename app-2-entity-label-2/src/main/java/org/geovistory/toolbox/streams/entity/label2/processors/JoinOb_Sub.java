package org.geovistory.toolbox.streams.entity.label2.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.label2.stores.PEStore;
import org.geovistory.toolbox.streams.entity.label2.stores.SCompleteStore;

import java.util.Objects;

import static org.geovistory.toolbox.streams.entity.label2.lib.Fn.*;

public class JoinOb_Sub implements Processor<ProjectEntityKey, Integer, String, EdgeValue> {
    private KeyValueStore<ProjectStatementKey, StatementJoinValue> sCompleteStore;
    private KeyValueStore<ProjectEntityKey, EntityValue> peStore;

    private ProcessorContext<String, EdgeValue> context;

    @Override
    public void init(ProcessorContext<String, EdgeValue> context) {
        sCompleteStore = context.getStateStore(SCompleteStore.NAME);
        peStore = context.getStateStore(PEStore.NAME);
        this.context = context;
    }

    @Override
    public void process(Record<ProjectEntityKey, Integer> record) {
        var statementId = record.value();
        var projectId = record.key().getProjectId();

        // create key
        var key = ProjectStatementKey.newBuilder()
                .setStatementId(statementId)
                .setProjectId(projectId)
                .build();

        // lookup project entity in peStore
        var pe = peStore.get(record.key());

        // lookup old complete statement
        var oldJoinVal = this.sCompleteStore.get(key);
        var oldPe = oldJoinVal != null ? oldJoinVal.getObjectEntityValue() : null;

        // if old and new differ
        if (!Objects.equals(pe, oldPe)) {

            StatementJoinValue newJoinVal;

            // create new join value
            if (oldJoinVal == null) {
                newJoinVal = StatementJoinValue.newBuilder()
                        .setStatementId(statementId)
                        .setProjectId(projectId)
                        .setObjectEntityValue(pe)
                        .build();
            } else {
                oldJoinVal.setObjectEntityValue(pe);
                newJoinVal = oldJoinVal;
            }

            // update the sCompleteStore
            this.sCompleteStore.put(key, newJoinVal);

            var edgeOutV = createOutgoingEdge(newJoinVal);
            var edgeOutK = createEdgeKey(edgeOutV);

            var edgeInV = createIncomingEdge(newJoinVal);
            var edgeInK = createEdgeKey(edgeInV);

            // push downstream
            this.context.forward(record.withKey(edgeOutK).withValue(edgeOutV));
            this.context.forward(record.withKey(edgeInK).withValue(edgeInV));


        }
    }
}


