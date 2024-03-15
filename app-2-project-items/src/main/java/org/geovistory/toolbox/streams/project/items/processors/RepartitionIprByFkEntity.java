package org.geovistory.toolbox.streams.project.items.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class RepartitionIprByFkEntity implements Processor<ts.projects.info_proj_rel.Key, ts.projects.info_proj_rel.Value, Integer, ts.projects.info_proj_rel.Value> {
    private ProcessorContext<Integer, ts.projects.info_proj_rel.Value> context;

    public void init(ProcessorContext<Integer, ts.projects.info_proj_rel.Value> context) {
        this.context = context;
    }

    public void process(Record<ts.projects.info_proj_rel.Key, ts.projects.info_proj_rel.Value> record) {
        var newKey = record.value().getFkEntity();
        this.context.forward(record.withKey(newKey));
    }
}