package org.geovistory.toolbox.streams.entity.label2;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import ts.information.resource.Key;
import ts.information.resource.Value;

class TestProcessor implements Processor<Key, Value, Key, Value> {

    private ProcessorContext<Key, Value> context;


    @Override
    public void process(Record<Key, Value> record) {
        context.forward(record);
    }

    @Override
    public void init(ProcessorContext<Key, Value> context) {
        Processor.super.init(context);
        this.context = context;
    }

    @Override
    public void close() {
        // close any resources managed by this processor
        // Note: Do not close any StateStores as these are managed by the library
    }

}