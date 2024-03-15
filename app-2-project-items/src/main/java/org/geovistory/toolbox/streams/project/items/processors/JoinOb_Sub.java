package org.geovistory.toolbox.streams.project.items.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.EdgeValue;
import org.geovistory.toolbox.streams.avro.ProjectStatementKey;
import org.geovistory.toolbox.streams.avro.StatementJoinValue;
import org.geovistory.toolbox.streams.avro.StatementWithObValue;
import org.geovistory.toolbox.streams.project.items.lib.Fn;
import org.geovistory.toolbox.streams.project.items.stores.SCompleteStore;

import java.util.Objects;

public class JoinOb_Sub implements Processor<ProjectStatementKey, StatementWithObValue, String, EdgeValue> {
    private KeyValueStore<ProjectStatementKey, StatementJoinValue> sCompleteStore;

    private ProcessorContext<String, EdgeValue> context;

    @Override
    public void init(ProcessorContext<String, EdgeValue> context) {
        sCompleteStore = context.getStateStore(SCompleteStore.NAME);
        this.context = context;
    }

    @Override
    public void process(Record<ProjectStatementKey, StatementWithObValue> record) {
        var statementId = record.key().getStatementId();
        var projectId = record.key().getProjectId();
        var newPe = record.value() != null ? record.value().getObjectEntityValue() : null;

        // lookup old complete statement
        var oldJoinVal = this.sCompleteStore.get(record.key());
        var oldPe = oldJoinVal != null ? oldJoinVal.getObjectEntityValue() : null;

        // if old and new differ
        if (!Objects.equals(newPe, oldPe)) {

            StatementJoinValue newJoinVal;

            // create new join value
            if (oldJoinVal == null) {
                newJoinVal = StatementJoinValue.newBuilder()
                        .setStatementId(statementId)
                        .setProjectId(projectId)
                        .setObjectEntityValue(newPe)
                        .build();
            } else {
                oldJoinVal.setObjectEntityValue(newPe);
                newJoinVal = oldJoinVal;
            }

            // update the sCompleteStore
            this.sCompleteStore.put(record.key(), newJoinVal);

            // ensure the join value is complete, i.e. subject has been joined
            if (newJoinVal.getSubject() != null) {

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
}


