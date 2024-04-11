package org.geovistory.toolbox.streams.entity.processors;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.EdgeValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.TimePrimitive;
import org.geovistory.toolbox.streams.avro.TimeSpanValue;
import org.geovistory.toolbox.streams.entity.lib.Fn;
import org.geovistory.toolbox.streams.entity.lib.TimeSpanFactory;
import org.geovistory.toolbox.streams.entity.stores.TimePrimitiveSortKeyStore;
import org.geovistory.toolbox.streams.entity.stores.TimePrimitiveStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static org.geovistory.toolbox.streams.entity.lib.Fn.createProjectSourceEntityKey;
import static org.geovistory.toolbox.streams.lib.Utils.createEdgeUniqueKey;

public class CreateTimeSpans implements Processor<String, EdgeValue, ProjectEntityKey, TimeSpanValue> {
    private static final Logger LOG = LoggerFactory.getLogger(CreateTimeSpans.class);

    private ProcessorContext<ProjectEntityKey, TimeSpanValue> context;
    private KeyValueStore<String, TimePrimitive> timePrimitiveStore;
    private KeyValueStore<String, String> sortKeyStore;

    private final String slug;

    public CreateTimeSpans(String slug) {
        this.slug = slug + "_";
    }

    public void init(ProcessorContext<ProjectEntityKey, TimeSpanValue> context) {
        this.context = context;
        timePrimitiveStore = context.getStateStore(TimePrimitiveStore.NAME);
        sortKeyStore = context.getStateStore(TimePrimitiveSortKeyStore.NAME);

    }

    public void process(Record<String, EdgeValue> record) {
        LOG.debug("process() called with record: {}", record);
        if (record.value() == null) return;
        if (record.value().getTargetNode().getTimePrimitive() == null) return;
        var tp = record.value().getTargetNode().getTimePrimitive();

        var edgeId = slug + createEdgeUniqueKey(record.value());
        var sortKeyOld = sortKeyStore.get(edgeId);
        var sortKeyNew = slug + Fn.createEdgeSortableKey(record.value());
        var positionChanged = !Objects.equals(sortKeyOld, sortKeyNew);
        // if deleted or changed
        if ((record.value().getDeleted() || positionChanged) && sortKeyOld != null) {
            // delete edge from old position in sorted store
            timePrimitiveStore.delete(sortKeyOld);
        }
        // delete sort key for edge
        if (record.value().getDeleted()) sortKeyStore.delete(edgeId);

        if (!record.value().getDeleted()) {
            // put edge at new position in sorted store
            timePrimitiveStore.put(sortKeyNew, tp);
            // update sort key for edge
            sortKeyStore.put(edgeId, sortKeyNew);
        }


        var projectId = record.value().getProjectId();
        var entityId = record.value().getSourceId();

        var k = createProjectSourceEntityKey(record.value());
        var v = createTimeSpan(projectId, entityId);
        context.forward(record.withKey(k).withValue(v));
    }


    private TimeSpanValue createTimeSpan(int projectId, String entityId) {

        var p = new TimeSpanFactory.Parser();

        var props = new Integer[]{71, 72, 150, 151, 152, 153};

        for (Integer propertyId : props) {
            var prefix = slug + Fn.createEdgePrefix(projectId, entityId, propertyId, true);
            try (var iterator = timePrimitiveStore.prefixScan(prefix, Serdes.String().serializer())) {
                if (iterator.hasNext()) {
                    // take the first time primitive per property
                    p.processTimePrimitive(propertyId, iterator.next().value);
                }
            }
        }

        return p.getTimeSpanValue();

    }

}
