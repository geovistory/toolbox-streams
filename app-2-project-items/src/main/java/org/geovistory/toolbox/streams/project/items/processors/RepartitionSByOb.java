package org.geovistory.toolbox.streams.project.items.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectStatementKey;
import org.geovistory.toolbox.streams.avro.StatementValue;

public class RepartitionSByOb implements Processor<ProjectStatementKey, StatementValue, ProjectEntityKey, Integer> {
    private ProcessorContext<ProjectEntityKey, Integer> context;

    public void init(ProcessorContext<ProjectEntityKey, Integer> context) {
        this.context = context;
    }

    public void process(Record<ProjectStatementKey, StatementValue> record) {

        // Only push statements downstream that have a subject entity and an object entity.
        // No statements with object literal.
        if (record.value().getSubject() != null &&
                record.value().getObject() != null &&
                record.value().getSubject().getEntity() != null &&
                record.value().getObject().getEntity() != null) {

            var newKey = ProjectEntityKey.newBuilder()
                    .setEntityId(record.value().getObjectId())
                    .setProjectId(record.value().getProjectId()).build();
            this.context.forward(record.withKey(newKey).withValue(record.value().getStatementId()));
        }
    }
}