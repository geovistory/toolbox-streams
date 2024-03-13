package org.geovistory.toolbox.streams.entity.label2.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.EntityProjectedValue;
import org.geovistory.toolbox.streams.avro.IprJoinVal;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.StatementEnrichedValue;
import org.geovistory.toolbox.streams.entity.label2.stores.EStore;
import org.geovistory.toolbox.streams.entity.label2.stores.IprStore;
import org.geovistory.toolbox.streams.entity.label2.stores.SStore;
import ts.projects.info_proj_rel.Key;
import ts.projects.info_proj_rel.Value;

import static org.geovistory.toolbox.streams.entity.label2.lib.Fn.createIprJoinValue;
import static org.geovistory.toolbox.streams.entity.label2.lib.Fn.createProjectEntityKey;
import static org.geovistory.toolbox.streams.entity.label2.names.ProcessorNames.IPR_TO_E;
import static org.geovistory.toolbox.streams.entity.label2.names.ProcessorNames.IPR_TO_S;

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
        var newIprJoinVal = createIprJoinValue(record.value(), entity, statement);

        // if old and new differ
        if (!newIprJoinVal.equals(oldIprJoinVal)) {

            // update the iprStore
            this.iprStore.put(key, newIprJoinVal);
            var k = createProjectEntityKey(newIprJoinVal.getIpr());

            // if the new join val has a statement value
            if (newIprJoinVal.getS() != null) {

                // push downstream
                this.context.forward(record.withKey(k).withValue(newIprJoinVal), IPR_TO_S);
            }
            // if the new join val has an entity value
            else if (newIprJoinVal.getE() != null) {

                // push downstream
                this.context.forward(record.withKey(k).withValue(newIprJoinVal), IPR_TO_E);

            }


        }
    }
}
