package org.geovistory.toolbox.streams.entity.label3.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.geovistory.toolbox.streams.avro.EntityLabel;
import org.geovistory.toolbox.streams.avro.EntityLabelOperation;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectLabelGroupKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.geovistory.toolbox.streams.entity.label3.lib.Fn.createEntityLabel;

public class ReKeyEntityLabels implements Processor<ProjectLabelGroupKey, EntityLabelOperation, ProjectEntityKey, EntityLabel> {
    private static final Logger LOG = LoggerFactory.getLogger(ReKeyEntityLabels.class);
    private ProcessorContext<ProjectEntityKey, EntityLabel> context;

    public void init(ProcessorContext<ProjectEntityKey, EntityLabel> context) {
        this.context = context;
    }

    public void process(Record<ProjectLabelGroupKey, EntityLabelOperation> record) {
        LOG.debug("process() called with record: {}", record);
        var k = ProjectEntityKey.newBuilder()
                .setEntityId(record.key().getEntityId())
                .setProjectId(record.key().getProjectId())
                .build();
        var v = createEntityLabel(record.value());
        this.context.forward(record.withKey(k).withValue(v));
    }


}