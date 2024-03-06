package org.geovistory.toolbox.streams.entity.label2.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import ts.information.resource.Key;

public class RepartitionIprByFkEntity implements Processor<ts.projects.info_proj_rel.Key, ts.projects.info_proj_rel.Value, ts.information.resource.Key, ts.projects.info_proj_rel.Value> {
    private ProcessorContext<ts.information.resource.Key, ts.projects.info_proj_rel.Value> context;

    public void init(ProcessorContext<ts.information.resource.Key, ts.projects.info_proj_rel.Value> context) {
        this.context = context;
    }

    public void process(Record<ts.projects.info_proj_rel.Key, ts.projects.info_proj_rel.Value> record) {
        Key newKey = ts.information.resource.Key.newBuilder().setPkEntity(record.value().getFkEntity()).build();
        this.context.forward(record.withKey(newKey));
    }
}