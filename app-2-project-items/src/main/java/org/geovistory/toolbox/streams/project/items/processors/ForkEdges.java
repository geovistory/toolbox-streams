package org.geovistory.toolbox.streams.project.items.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.EdgeValue;
import org.geovistory.toolbox.streams.avro.EdgeVisibilityValue;
import org.geovistory.toolbox.streams.project.items.lib.ExtractVisiblity;
import org.geovistory.toolbox.streams.project.items.names.ProcessorNames;
import org.geovistory.toolbox.streams.project.items.names.SinkNames;
import org.geovistory.toolbox.streams.project.items.stores.EdgeVisibilityStore;

import static org.geovistory.toolbox.streams.project.items.lib.Fn.createEdge;


public class ForkEdges implements Processor<String, EdgeVisibilityValue, String, EdgeValue> {

    private ProcessorContext<String, EdgeValue> context;
    private KeyValueStore<String, EdgeVisibilityValue> edgeVisStore;


    @Override
    public void init(ProcessorContext<String, EdgeValue> context) {
        edgeVisStore = context.getStateStore(EdgeVisibilityStore.NAME);
        this.context = context;
    }

    public void process(Record<String, EdgeVisibilityValue> record) {

        EdgeVisibilityValue oldEdgeVis = edgeVisStore.get(record.key());
        var newEdgeVis = record.value();
        var newEdge = createEdge(newEdgeVis);

        // forward to project toolbox
        context.forward(record.withValue(newEdge), SinkNames.PROJECT_EDGE_TOOLBOX_SINK);

        // if oldEdge null, forward where visible
        if (oldEdgeVis == null) {

            if (newEdgeVis.getProjectPublic())
                context.forward(record.withValue(newEdge), SinkNames.PROJECT_EDGE_PUBLIC_SINK);

            if (newEdgeVis.getCommunityToolbox())
                context.forward(record.withValue(newEdge), ProcessorNames.CREATE_COMMUNITY_TOOLBOX_EDGES);

            if (newEdgeVis.getCommunityPublic())
                context.forward(record.withValue(newEdge), ProcessorNames.CREATE_COMMUNITY_PUBLIC_EDGES);

        } else {

            forwardToTarget(record, oldEdgeVis, newEdgeVis, newEdge,
                    EdgeVisibilityValue::getProjectPublic, SinkNames.PROJECT_EDGE_PUBLIC_SINK
            );

            forwardToTarget(record, oldEdgeVis, newEdgeVis, newEdge,
                    EdgeVisibilityValue::getCommunityToolbox, ProcessorNames.CREATE_COMMUNITY_TOOLBOX_EDGES
            );

            forwardToTarget(record, oldEdgeVis, newEdgeVis, newEdge,
                    EdgeVisibilityValue::getCommunityPublic, ProcessorNames.CREATE_COMMUNITY_PUBLIC_EDGES
            );


        }
        edgeVisStore.put(record.key(), record.value());

    }

    private void forwardToTarget(
            Record<String, EdgeVisibilityValue> record,
            EdgeVisibilityValue oldEdgeVis,
            EdgeVisibilityValue newEdgeVis,
            EdgeValue newEdge,
            ExtractVisiblity visibility,
            String childName) {
        var turnedVisible = !visibility.get(oldEdgeVis) && visibility.get(newEdgeVis);
        var turnedHidden = visibility.get(oldEdgeVis) && !visibility.get(newEdgeVis);
        var addedToProject = oldEdgeVis.getDeleted() && !newEdgeVis.getDeleted();
        var removedFromProject = !oldEdgeVis.getDeleted() && newEdgeVis.getDeleted();
        var res = EdgeValue.newBuilder(newEdge).build();
        if (turnedVisible || addedToProject)
            res.setDeleted(false);
        else if (turnedHidden || removedFromProject) {
            res.setDeleted(true);
        }
        context.forward(record.withValue(res), childName);
    }


}
