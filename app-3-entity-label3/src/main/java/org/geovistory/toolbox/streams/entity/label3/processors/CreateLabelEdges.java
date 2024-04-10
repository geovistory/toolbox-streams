package org.geovistory.toolbox.streams.entity.label3.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.geovistory.toolbox.streams.avro.EdgeValue;
import org.geovistory.toolbox.streams.avro.LabelEdge;
import org.geovistory.toolbox.streams.entity.label3.names.Sinks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.geovistory.toolbox.streams.entity.label3.lib.Fn.createLabelEdge;

public class CreateLabelEdges implements Processor<String, EdgeValue, String, LabelEdge> {
    private static final Logger LOG = LoggerFactory.getLogger(CreateLabelEdges.class);
    private ProcessorContext<String, LabelEdge> context;

    public void init(ProcessorContext<String, LabelEdge> context) {
        this.context = context;
    }

    public void process(Record<String, EdgeValue> record) {
        LOG.debug("process() called with record: {}", record);

        LabelEdge newVal = createLabelEdge(record.value());

        var targetIsLiteral = record.value().getTargetNode().getEntity() == null;
        if (targetIsLiteral) this.context.forward(record.withValue(newVal), Sinks.LABEL_EDGE_BY_SOURCE);
        else this.context.forward(record.withValue(newVal), Sinks.LABEL_EDGE_BY_TARGET);
    }





}