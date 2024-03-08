package org.geovistory.toolbox.streams.entity.label2.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class RepartitionEByPk implements Processor<ts.information.resource.Key, ts.information.resource.Value, Integer, ts.information.resource.Value> {
    private ProcessorContext<Integer, ts.information.resource.Value> context;

    public void init(ProcessorContext<Integer, ts.information.resource.Value> context) {
        this.context = context;
    }

    public void process(Record<ts.information.resource.Key, ts.information.resource.Value> record) {
        var newKey = record.value().getPkEntity();
        this.context.forward(record.withKey(newKey));
    }
}