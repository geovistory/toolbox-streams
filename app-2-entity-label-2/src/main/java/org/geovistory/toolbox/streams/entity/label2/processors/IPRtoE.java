package org.geovistory.toolbox.streams.entity.label2.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.geovistory.toolbox.streams.avro.EntityValue;
import org.geovistory.toolbox.streams.avro.IprJoinVal;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;

import static org.geovistory.toolbox.streams.entity.label2.lib.Fn.createEntityValue;
import static org.geovistory.toolbox.streams.entity.label2.lib.Fn.createProjectEntityKey;

public class IPRtoE implements Processor<ProjectEntityKey, IprJoinVal, ProjectEntityKey, EntityValue> {

    private ProcessorContext<ProjectEntityKey, EntityValue> context;

    public void init(ProcessorContext<ProjectEntityKey, EntityValue> context) {
        this.context = context;
    }

    public void process(Record<ProjectEntityKey, IprJoinVal> record) {
        var vIn = record.value();
        var k = createProjectEntityKey(vIn.getIpr());
        var v = createEntityValue(vIn.getE(), vIn.getIpr());
        // push downstream
        this.context.forward(record.withKey(k).withValue(v));
    }
}
