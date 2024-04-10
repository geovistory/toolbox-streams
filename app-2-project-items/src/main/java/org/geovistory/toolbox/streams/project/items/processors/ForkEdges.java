package org.geovistory.toolbox.streams.project.items.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.EdgeValue;
import org.geovistory.toolbox.streams.avro.EdgeVisibilityValue;
import org.geovistory.toolbox.streams.avro.Entity;
import org.geovistory.toolbox.streams.avro.EntityValue;
import org.geovistory.toolbox.streams.project.items.lib.ExtractVisiblity;
import org.geovistory.toolbox.streams.project.items.names.ProcessorNames;
import org.geovistory.toolbox.streams.project.items.names.SinkNames;
import org.geovistory.toolbox.streams.project.items.stores.EdgeVisibilityStore;
import org.geovistory.toolbox.streams.project.items.stores.EntityBoolStore;

import static org.geovistory.toolbox.streams.project.items.lib.Fn.createEdge;
import static org.geovistory.toolbox.streams.project.items.lib.Fn.getOperation;


public class ForkEdges implements Processor<String, EdgeVisibilityValue, String, EdgeValue> {

    private ProcessorContext<String, EdgeValue> context;
    private KeyValueStore<String, EdgeVisibilityValue> edgeVisStore;
    private KeyValueStore<String, Boolean> boolStore;


    @Override
    public void init(ProcessorContext<String, EdgeValue> context) {
        edgeVisStore = context.getStateStore(EdgeVisibilityStore.NAME);
        boolStore = context.getStateStore(EntityBoolStore.NAME);
        this.context = context;
    }

    public void process(Record<String, EdgeVisibilityValue> record) {

        EdgeVisibilityValue oldEdgeVis = edgeVisStore.get(record.key());
        var newEdgeVis = record.value();
        var newEdge = createEdge(newEdgeVis);
        var modified = edgesDiffer(newEdge, oldEdgeVis == null ? null : createEdge(oldEdgeVis));

        forward(
                record,
                newEdge,
                (e) -> true,
                EntityBoolStore.createTargetKey("edg_tp", newEdgeVis),
                modified,
                newEdge.getDeleted(),
                SinkNames.TOOLBOX_PROJECT_EDGE_SINK
        );
        forward(
                record,
                newEdge,
                EdgeVisibilityValue::getProjectPublic,
                EntityBoolStore.createTargetKey("edg_pp", newEdgeVis),
                modified,
                newEdge.getDeleted(),
                SinkNames.PUBLIC_PROJECT_EDGE_SINK
        );
        forward(
                record,
                newEdge,
                EdgeVisibilityValue::getCommunityToolbox,
                EntityBoolStore.createTargetKey("edg_tc", newEdgeVis),
                modified,
                newEdge.getDeleted(),
                ProcessorNames.TOOLBOX_CREATE_COMMUNITY_EDGES
        );

        forward(
                record,
                newEdge,
                EdgeVisibilityValue::getCommunityPublic,
                EntityBoolStore.createTargetKey("edg_pc", newEdgeVis),
                modified,
                newEdge.getDeleted(),
                ProcessorNames.PUBLIC_CREATE_COMMUNITY_EDGES
        );

        edgeVisStore.put(record.key(), record.value());

    }


    public void forward(
            Record<String, EdgeVisibilityValue> record,
            EdgeValue edgeValue,
            ExtractVisiblity<EdgeVisibilityValue> visibility,
            String targetKey,
            boolean modified,
            boolean deleted,
            String childName) {
        var op = getOperation(
                this.boolStore,
                record,
                visibility,
                targetKey,
                modified,
                deleted
        );

        switch (op) {
            case DELETE -> {
                edgeValue.setDeleted(true);
                context.forward(record.withValue(edgeValue), childName);
            }
            case INSERT, UPDATE -> {
                edgeValue.setDeleted(false);
                context.forward(record.withValue(edgeValue), childName);
            }
        }

    }

    private static boolean edgesDiffer(EdgeValue a, EdgeValue b) {
        if (a == null && b != null) return false;
        if (a != null && b == null) return false;
        if (a == null) return true;

        var aClean = resetVisibilityValues(a);
        var bClean = resetVisibilityValues(b);

        // compare edges independent of the visibility values
        return !aClean.equals(bClean);
    }

    private static EdgeValue resetVisibilityValues(EdgeValue a) {
        var copy = EdgeValue.newBuilder(a).build();
        resetProjectEntityVis(copy.getTargetProjectEntity());
        resetProjectEntityVis(copy.getSourceProjectEntity());
        resetEntityVis(copy.getSourceEntity());
        resetEntityVis(copy.getTargetNode() == null ? null : copy.getTargetNode().getEntity());
        return copy;
    }

    private static void resetProjectEntityVis(EntityValue e) {
        if (e != null) {
            e.setCommunityVisibilityDataApi(true);
            e.setCommunityVisibilityWebsite(true);
            e.setCommunityVisibilityToolbox(true);
            e.setProjectVisibilityDataApi(true);
            e.setProjectVisibilityWebsite(true);
        }
    }

    private static void resetEntityVis(Entity e) {
        if (e != null) {
            e.setCommunityVisibilityDataApi(true);
            e.setCommunityVisibilityWebsite(true);
            e.setCommunityVisibilityToolbox(true);
        }
    }


}
