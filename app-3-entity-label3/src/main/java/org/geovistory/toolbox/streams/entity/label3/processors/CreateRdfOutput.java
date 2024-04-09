package org.geovistory.toolbox.streams.entity.label3.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.geovistory.toolbox.streams.avro.EntityLabelOperation;
import org.geovistory.toolbox.streams.avro.Operation;
import org.geovistory.toolbox.streams.avro.ProjectLabelGroupKey;
import org.geovistory.toolbox.streams.avro.ProjectRdfValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateRdfOutput implements Processor<ProjectLabelGroupKey, EntityLabelOperation, ProjectLabelGroupKey, ProjectRdfValue> {
    private static final Logger LOG = LoggerFactory.getLogger(CreateRdfOutput.class);
    private ProcessorContext<ProjectLabelGroupKey, ProjectRdfValue> context;

    public void init(ProcessorContext<ProjectLabelGroupKey, ProjectRdfValue> context) {
        this.context = context;
    }

    public void process(Record<ProjectLabelGroupKey, EntityLabelOperation> record) {
        LOG.debug("process() called with record: {}", record);
        var op = ProjectRdfValue.newBuilder().setOperation(
                record.value().getDeleted() ? Operation.delete : Operation.insert
        ).build();
        this.context.forward(record.withValue(op));
    }


}