package org.geovistory.toolbox.streams.entity.label3.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.geovistory.toolbox.streams.avro.EntityLabelConfig;
import org.geovistory.toolbox.streams.avro.EntityLabelConfigTmstp;
import org.geovistory.toolbox.streams.avro.ProjectClassKey;
import org.geovistory.toolbox.streams.lib.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ts.projects.entity_label_config.Key;
import ts.projects.entity_label_config.Value;

public class LabelConfigTransformer implements Processor<Key, Value, ProjectClassKey, EntityLabelConfigTmstp> {
    private static final Logger LOG = LoggerFactory.getLogger(LabelConfigTransformer.class);
    private ProcessorContext<ProjectClassKey, EntityLabelConfigTmstp> context;
    private final ObjectMapper mapper = new ObjectMapper(); // create once, reuse

    public void init(ProcessorContext<ProjectClassKey, EntityLabelConfigTmstp> context) {
        this.context = context;
    }

    public void process(Record<Key, Value> record) {
        LOG.info("process() called with record: {}", record);
        var value = record.value();
        try {
            EntityLabelConfig config = mapper.readValue(value.getConfig(), EntityLabelConfig.class);
            var k = ProjectClassKey.newBuilder()
                    .setClassId(value.getFkClass())
                    .setProjectId(value.getFkProject())
                    .build();
            var v = EntityLabelConfigTmstp.newBuilder()
                    .setClassId(value.getFkClass())
                    .setProjectId(value.getFkProject())
                    .setConfig(config)
                    .setDeleted(Utils.stringIsEqualTrue(value.getDeleted$1()))
                    .setRecordTimestamp(record.timestamp())
                    .build();

            // push downstream
            context.forward(record.withKey(k).withValue(v));
        } catch (Exception ignore) {
        }


    }


}