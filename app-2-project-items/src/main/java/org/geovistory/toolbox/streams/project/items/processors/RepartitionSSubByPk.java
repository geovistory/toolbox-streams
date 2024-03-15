package org.geovistory.toolbox.streams.project.items.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectStatementKey;
import org.geovistory.toolbox.streams.avro.StatementWithSubValue;

public class RepartitionSSubByPk implements Processor<ProjectEntityKey, StatementWithSubValue, ProjectStatementKey, StatementWithSubValue> {

    private ProcessorContext<ProjectStatementKey, StatementWithSubValue> context;

    public void init(ProcessorContext<ProjectStatementKey, StatementWithSubValue> context) {
        this.context = context;
    }

    /**
     * We repartition statements with a object entity by statement id and project id
     *
     * @param record the record to process
     */
    public void process(Record<ProjectEntityKey, StatementWithSubValue> record) {

        // only take statements having an entity as object (not statements with literal)
        if (record.value().getObject() != null && record.value().getObject().getEntity() != null) {
            // push downstream
            this.context.forward(record.withKey(
                    ProjectStatementKey.newBuilder()
                            .setStatementId(record.value().getStatementId())
                            .setProjectId(record.value().getProjectId()).build()
            ));
        }
    }


}
