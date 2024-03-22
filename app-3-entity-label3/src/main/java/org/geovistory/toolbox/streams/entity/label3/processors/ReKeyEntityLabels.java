package org.geovistory.toolbox.streams.entity.label3.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.geovistory.toolbox.streams.avro.EntityLabel;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectLabelGroupKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReKeyEntityLabels implements Processor<ProjectLabelGroupKey, EntityLabel, ProjectEntityKey, EntityLabel> {
    private static final Logger LOG = LoggerFactory.getLogger(ReKeyEntityLabels.class);
    private ProcessorContext<ProjectEntityKey, EntityLabel> context;

    public void init(ProcessorContext<ProjectEntityKey, EntityLabel> context) {
        this.context = context;
    }

    public void process(Record<ProjectLabelGroupKey, EntityLabel> record) {
        LOG.info("process() called with record: {}", record);
        var k = ProjectEntityKey.newBuilder().setEntityId(record.key().getEntityId())
                .setProjectId(record.key().getProjectId()).build();
        this.context.forward(record.withKey(k));
    }


}