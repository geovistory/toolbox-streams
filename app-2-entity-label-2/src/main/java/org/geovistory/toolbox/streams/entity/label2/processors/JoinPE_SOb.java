package org.geovistory.toolbox.streams.entity.label2.processors;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.EntityValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectStatementKey;
import org.geovistory.toolbox.streams.entity.label2.lib.FileWriter;
import org.geovistory.toolbox.streams.entity.label2.stores.SObStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JoinPE_SOb implements Processor<ProjectEntityKey, EntityValue, ProjectStatementKey, EntityValue> {
    private static final Logger LOG = LoggerFactory.getLogger(FileWriter.class);

    private KeyValueStore<String, EntityValue> sObStore;

    private ProcessorContext<ProjectStatementKey, EntityValue> context;

    @Override
    public void init(ProcessorContext<ProjectStatementKey, EntityValue> context) {
        sObStore = context.getStateStore(SObStore.NAME);
        this.context = context;
    }

    @Override
    public void process(Record<ProjectEntityKey, EntityValue> record) {
        var k = record.key();
        var newV = record.value();

        // create prefix in form of {fk_entity}_{fk_project}_
        var prefix = k.getEntityId() + "_" + k.getProjectId() + "_";

        // scan statements with subject store with prefix
        var iterator = this.sObStore.prefixScan(prefix, Serdes.String().serializer());

        // iterate over all matches in iprStore
        while (iterator.hasNext()) {
            // get key-value record of a statement with subject
            var s = iterator.next();

            // get old joined project entity
            var oldV = s.value;

            // if new differs old
            if (!newV.equals(oldV)) {

                // update project entity in subject store
                sObStore.put(s.key, newV);


                // push downstream
                try {
                    var statementId = Integer.parseInt(s.key.replace(prefix, ""));
                    var outK = ProjectStatementKey.newBuilder()
                            .setProjectId(k.getProjectId())
                            .setStatementId(statementId).build();
                    this.context.forward(record.withKey(outK).withValue(newV));
                } catch (NumberFormatException e) {
                    LOG.warn("Unable to extract statementId from this key: {}", s.key);
                }

            }
        }
    }
}


