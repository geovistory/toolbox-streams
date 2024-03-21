package org.geovistory.toolbox.streams.entity.label3.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.geovistory.toolbox.streams.avro.EntityLabel;
import org.geovistory.toolbox.streams.avro.ProjectEntityLangKey;
import org.geovistory.toolbox.streams.avro.ProjectLabelGroupKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReKeyEntityLangLabels implements Processor<ProjectLabelGroupKey, EntityLabel, ProjectEntityLangKey, EntityLabel> {
    private static final Logger LOG = LoggerFactory.getLogger(ReKeyEntityLangLabels.class);
    private ProcessorContext<ProjectEntityLangKey, EntityLabel> context;

    public void init(ProcessorContext<ProjectEntityLangKey, EntityLabel> context) {
        this.context = context;
    }

    public void process(Record<ProjectLabelGroupKey, EntityLabel> record) {
        LOG.debug("process() called with record: {}", record);
        var k = ProjectEntityLangKey.newBuilder()
                .setEntityId(record.key().getEntityId())
                .setProjectId(record.key().getProjectId())
                .setLanguage(record.key().getLanguage())
                .build();
        this.context.forward(record.withKey(k));
    }


}