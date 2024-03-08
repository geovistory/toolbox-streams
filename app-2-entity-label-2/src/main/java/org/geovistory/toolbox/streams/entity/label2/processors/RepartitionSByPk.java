package org.geovistory.toolbox.streams.entity.label2.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.geovistory.toolbox.streams.avro.StatementEnrichedValue;

public class RepartitionSByPk implements Processor<ts.information.statement.Key, StatementEnrichedValue, Integer, StatementEnrichedValue> {
    private ProcessorContext<Integer, StatementEnrichedValue> context;

    public void init(ProcessorContext<Integer, StatementEnrichedValue> context) {
        this.context = context;
    }

    public void process(Record<ts.information.statement.Key, StatementEnrichedValue> record) {
        var newKey = record.key().getPkEntity();
        this.context.forward(record.withKey(newKey));
    }
}