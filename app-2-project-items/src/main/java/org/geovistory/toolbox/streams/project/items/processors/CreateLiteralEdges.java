package org.geovistory.toolbox.streams.project.items.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.geovistory.toolbox.streams.avro.EdgeValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.StatementWithSubValue;
import org.geovistory.toolbox.streams.project.items.lib.Fn;

public class CreateLiteralEdges implements Processor<ProjectEntityKey, StatementWithSubValue, String, EdgeValue> {

    private ProcessorContext<String, EdgeValue> context;

    public void init(ProcessorContext<String, EdgeValue> context) {
        this.context = context;
    }

    public void process(Record<ProjectEntityKey, StatementWithSubValue> record) {
        var v = record.value();
        // only push statements with literal downstream. Not statements with entity.
        if (v.getObject() != null
                && v.getObject().getEntity() == null) {

            var edgeV = Fn.createEdge(v);
            String edgeK = Fn.createEdgeKey(edgeV);

            // push downstream
            this.context.forward(record.withKey(edgeK).withValue(edgeV));
        }
    }


}
