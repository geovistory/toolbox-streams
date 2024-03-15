package org.geovistory.toolbox.streams.project.items.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.EntityProjectedValue;
import org.geovistory.toolbox.streams.avro.IprJoinVal;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.StatementEnrichedValue;
import org.geovistory.toolbox.streams.project.items.lib.Fn;
import org.geovistory.toolbox.streams.project.items.names.ProcessorNames;
import org.geovistory.toolbox.streams.project.items.stores.EStore;
import org.geovistory.toolbox.streams.project.items.stores.IprStore;
import org.geovistory.toolbox.streams.project.items.stores.SStore;
import ts.projects.info_proj_rel.Key;
import ts.projects.info_proj_rel.Value;

public class JoinIPR_E implements Processor<Key, Value, ProjectEntityKey, IprJoinVal> {
    private KeyValueStore<String, IprJoinVal> iprStore;
    private KeyValueStore<Integer, EntityProjectedValue> eStore;
    private KeyValueStore<Integer, StatementEnrichedValue> sStore;
    private ProcessorContext<ProjectEntityKey, IprJoinVal> context;

    public void init(ProcessorContext<ProjectEntityKey, IprJoinVal> context) {
        iprStore = context.getStateStore(IprStore.NAME);
        eStore = context.getStateStore(EStore.NAME);
        sStore = context.getStateStore(SStore.NAME);
        this.context = context;
    }

    public void process(Record<ts.projects.info_proj_rel.Key, ts.projects.info_proj_rel.Value> record) {

        // create key prefixed with fk entity
        var key = IprStore.createIprStoreKey(record.value());

        // lookup statement in sStore
        var statement = sStore.get(record.value().getFkEntity());

        // lookup entity in eStore, if no statement found
        EntityProjectedValue entity = null;
        if (statement == null) entity = eStore.get(record.value().getFkEntity());

        // lookup old ipr
        var oldIprJoinVal = this.iprStore.get(key);

        // build new ipr
        var newIprJoinVal = Fn.createIprJoinValue(record.value(), entity, statement);

        // if old and new differ
        if (!newIprJoinVal.equals(oldIprJoinVal)) {

            // update the iprStore
            this.iprStore.put(key, newIprJoinVal);
            var k = Fn.createProjectEntityKey(newIprJoinVal.getIpr());

            // if the new join val has a statement value
            if (newIprJoinVal.getS() != null) {

                // push downstream
                this.context.forward(record.withKey(k).withValue(newIprJoinVal), ProcessorNames.IPR_TO_S);
            }
            // if the new join val has an entity value
            else if (newIprJoinVal.getE() != null) {

                // push downstream
                this.context.forward(record.withKey(k).withValue(newIprJoinVal), ProcessorNames.IPR_TO_E);

            }


        }
    }
}
