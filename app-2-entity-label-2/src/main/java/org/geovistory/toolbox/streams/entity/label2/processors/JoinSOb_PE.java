package org.geovistory.toolbox.streams.entity.label2.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.EntityValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectStatementKey;
import org.geovistory.toolbox.streams.entity.label2.stores.PEStore;
import org.geovistory.toolbox.streams.entity.label2.stores.SObStore;

import java.util.Objects;

public class JoinSOb_PE implements Processor<ProjectEntityKey, Integer, ProjectStatementKey, EntityValue> {
    private KeyValueStore<String, EntityValue> sObStore;
    private KeyValueStore<ProjectEntityKey, EntityValue> peStore;

    private ProcessorContext<ProjectStatementKey, EntityValue> context;

    @Override
    public void init(ProcessorContext<ProjectStatementKey, EntityValue> context) {
        sObStore = context.getStateStore(SObStore.NAME);
        peStore = context.getStateStore(PEStore.NAME);
        this.context = context;
    }

    @Override
    public void process(Record<ProjectEntityKey, Integer> record) {
        var statementId = record.value();
        var projectId = record.key().getProjectId();
        var objectId = record.key().getEntityId();

        // create key {objectId}_{project_id}_{statement_id}
        var key = SObStore.createKey(objectId, projectId, statementId);

        // lookup project entity in peStore
        var newJoinVal = peStore.get(record.key());

        // lookup old statement with sub
        var oldJoinVal = this.sObStore.get(key);


        // if old and new differ
        if (newJoinVal == null ||
                !Objects.equals(newJoinVal, oldJoinVal)
        ) {

            // update the sObStore
            this.sObStore.put(key, newJoinVal);


            // push downstream
            if (newJoinVal != null) {
                this.context.forward(record
                        .withKey(
                                ProjectStatementKey.newBuilder().setStatementId(statementId).setProjectId(projectId).build()
                        ).withValue(
                                newJoinVal
                        )
                );
            }

        }
    }
}

