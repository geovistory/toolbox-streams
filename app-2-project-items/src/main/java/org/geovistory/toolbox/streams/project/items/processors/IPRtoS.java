package org.geovistory.toolbox.streams.project.items.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.geovistory.toolbox.streams.avro.IprJoinVal;
import org.geovistory.toolbox.streams.avro.ProjectStatementKey;
import org.geovistory.toolbox.streams.avro.StatementValue;

import static org.geovistory.toolbox.streams.project.items.lib.Fn.createProjectStatementKey;
import static org.geovistory.toolbox.streams.project.items.lib.Fn.createStatementValue;

public class IPRtoS implements Processor<ProjectStatementKey, IprJoinVal, ProjectStatementKey, StatementValue> {

    private ProcessorContext<ProjectStatementKey, StatementValue> context;

    public void init(ProcessorContext<ProjectStatementKey, StatementValue> context) {
        this.context = context;
    }

    public void process(Record<ProjectStatementKey, IprJoinVal> record) {
        var vIn = record.value();
        var k = createProjectStatementKey(vIn.getIpr());
        var v = createStatementValue(vIn.getS(), vIn.getIpr());
        // push downstream
        this.context.forward(record.withKey(k).withValue(v));
    }
}
