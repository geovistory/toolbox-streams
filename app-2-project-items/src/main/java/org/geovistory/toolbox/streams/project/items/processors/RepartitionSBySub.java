package org.geovistory.toolbox.streams.project.items.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectStatementKey;
import org.geovistory.toolbox.streams.avro.StatementValue;

public class RepartitionSBySub implements Processor<ProjectStatementKey, StatementValue, ProjectEntityKey, StatementValue> {
    private ProcessorContext<ProjectEntityKey, StatementValue> context;

    public void init(ProcessorContext<ProjectEntityKey, StatementValue> context) {
        this.context = context;
    }

    public void process(Record<ProjectStatementKey, StatementValue> record) {

        // only push statements downstream that have a subject entity.
        if (record.value().getSubject() != null
                && record.value().getSubject().getEntity() != null) {
            var newKey = ProjectEntityKey.newBuilder()
                    .setEntityId(record.value().getSubjectId())
                    .setProjectId(record.value().getProjectId()).build();
            this.context.forward(record.withKey(newKey));
        }
    }
}