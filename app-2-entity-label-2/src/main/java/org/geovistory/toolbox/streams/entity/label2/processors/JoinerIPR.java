package org.geovistory.toolbox.streams.entity.label2.processors;

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
import ts.projects.info_proj_rel.Key;
import ts.projects.info_proj_rel.Value;

import static org.geovistory.toolbox.streams.entity.label2.lib.Fn.*;

public class JoinerIPR implements Processor<Key, Value, ProjectEntityKey, ProjectEntityVisibilitiesValue> {
    private KeyValueStore<String, IprJoinVal> iprStore;
    private KeyValueStore<ts.information.resource.Key, EntityValue> eStore;
    private ProcessorContext<ProjectEntityKey, ProjectEntityVisibilitiesValue> context;

    public void init(ProcessorContext<ProjectEntityKey, ProjectEntityVisibilitiesValue> context) {
        iprStore = context.getStateStore(IprStore.NAME);
        eStore = context.getStateStore(EStore.NAME);
        this.context = context;
    }

    public void process(Record<ts.projects.info_proj_rel.Key, ts.projects.info_proj_rel.Value> record) {
        // create key prefixed with fk entity
        var key = IprStore.createIprStoreKey(record.value());

        // lookup entity in eStore
        var entity = eStore.get(ts.information.resource.Key.newBuilder().setPkEntity(record.value().getFkEntity()).build());

        // lookup old ipr
        var oldIprJoinVal = this.iprStore.get(key);

        // build new ipr
        var newIprJoinVal = createIprJoinValue(record.value(), entity);

        // if old and new differ
        if (!newIprJoinVal.equals(oldIprJoinVal)) {

            // update the iprStore
            this.iprStore.put(key, newIprJoinVal);


            // if the new join val has an entity value
            if (newIprJoinVal.getE() != null) {

                // push downstream
                var k = createProjectEntityKey(newIprJoinVal.getIpr());
                var v = createProjectEntityValue(newIprJoinVal.getE(), newIprJoinVal.getIpr());
                this.context.forward(record.withKey(k).withValue(v));
            }
        }
    }
}
