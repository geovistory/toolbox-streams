package org.geovistory.toolbox.streams.entity.label3.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.InProjectOrdNum;
import org.geovistory.toolbox.streams.avro.LabelEdge;
import org.geovistory.toolbox.streams.entity.label3.stores.EdgeCountStore;
import org.geovistory.toolbox.streams.entity.label3.stores.EdgeOrdNumStore;
import org.geovistory.toolbox.streams.entity.label3.stores.EdgeSumStore;
import org.geovistory.toolbox.streams.entity.label3.stores.EdgeVisibilityStore;


public class CreateCommunityToolboxEdges implements Processor<String, LabelEdge, String, LabelEdge> {

    private ProcessorContext<String, LabelEdge> context;
    private KeyValueStore<String, Integer> edgeCountStore;
    private KeyValueStore<String, Float> edgeSumStore;
    private KeyValueStore<String, InProjectOrdNum> edgeOrdNumStore;
    private KeyValueStore<String, LabelEdge> edgeVisStore;


    @Override
    public void init(ProcessorContext<String, LabelEdge> context) {
        edgeVisStore = context.getStateStore(EdgeVisibilityStore.NAME);
        edgeCountStore = context.getStateStore(EdgeCountStore.NAME);
        edgeOrdNumStore = context.getStateStore(EdgeOrdNumStore.NAME);
        edgeSumStore = context.getStateStore(EdgeSumStore.NAME);
        this.context = context;
    }

    public void process(Record<String, LabelEdge> record) {
        if (record.value() == null) return;

        LabelEdge oldEdge = edgeVisStore.get(record.key());
        var newEdge = record.value();

        // if oldEdge null
        if (oldEdge == null) {
            // continue processing where visible
            if (newEdge.getEdgeCommunityToolbox())
                // continue process
                count(record.withValue(newEdge));
        } else {
            // determine deleted flag
            newEdge = setDeleted(oldEdge, newEdge);

            // continue process
            count(record.withValue(newEdge));
        }
        edgeVisStore.put(record.key(), record.value());

    }

    private LabelEdge setDeleted(
            LabelEdge oldEdgeVis,
            LabelEdge newEdgeVis
    ) {
        var turnedVisible = !oldEdgeVis.getEdgeCommunityToolbox() && newEdgeVis.getEdgeCommunityToolbox();
        var turnedHidden = oldEdgeVis.getEdgeCommunityToolbox() && !newEdgeVis.getEdgeCommunityToolbox();
        var addedToProject = oldEdgeVis.getDeleted() && !newEdgeVis.getDeleted();
        var removedFromProject = !oldEdgeVis.getDeleted() && newEdgeVis.getDeleted();
        var res = LabelEdge.newBuilder(newEdgeVis).build();
        if (turnedVisible || addedToProject)
            res.setDeleted(false);
        else if (turnedHidden || removedFromProject) {
            res.setDeleted(true);
        }
        return res;
    }


    public void count(Record<String, LabelEdge> record) {
        var newEdge = LabelEdge.newBuilder(record.value()).build();
        var edgeCountKey = EdgeCountStore.createKey(newEdge);
        var edgeSumKey = EdgeSumStore.createKey(newEdge);

        var edgeBoolKey = EdgeOrdNumStore.createKey(newEdge);
        var newIsInProject = !newEdge.getDeleted();
        var oldInProjectOrdNum = edgeOrdNumStore.get(edgeBoolKey);
        edgeOrdNumStore.put(edgeBoolKey,
                InProjectOrdNum.newBuilder().setInProject(newIsInProject)
                        .setOrdNum(newEdge.getOrdNum())
                        .build()
        );
        boolean wasInProject;
        Float oldOrnNum;
        if (oldInProjectOrdNum == null) {
            wasInProject = false;
            oldOrnNum = null;
        } else {
            wasInProject = oldInProjectOrdNum.getInProject();
            oldOrnNum = oldInProjectOrdNum.getOrdNum();
        }
        var added = !wasInProject && newIsInProject;
        var removed = wasInProject && !newIsInProject;

        var newCount = createCount(newEdge, edgeCountKey, added, removed);
        createAverageOrdNum(newEdge, oldOrnNum, edgeSumKey, removed, newCount);


        newEdge.setProjectId(0);
        context.forward(record.withKey(edgeCountKey).withValue(newEdge));
    }

    private Float createAverageOrdNum(
            LabelEdge newEdge,
            Float oldOrdNum,
            String edgeSumKey,
            boolean removed,
            int newCount) {
        Float newOrdNum = newEdge.getOrdNum();
        var sum = edgeSumStore.get(edgeSumKey);
        if (oldOrdNum != null) {
            sum = sum != null ? sum : 0;
            sum = sum - oldOrdNum;
        }
        if (newOrdNum != null && !removed) {
            sum = sum != null ? sum : 0;
            sum = sum + newOrdNum;
        }
        edgeSumStore.put(edgeSumKey, sum);
        var newAvg = newCount > 0
                ? sum != null ? sum / newCount : null
                : null;
        newEdge.setOrdNum(newAvg);
        return newAvg;
    }

    private int createCount(LabelEdge newEdge, String edgeCountKey, boolean added, boolean removed) {
        var diff = added ? 1 : removed ? -1 : 0;

        Integer oldCount = edgeCountStore.get(edgeCountKey);
        oldCount = oldCount != null ? oldCount : 0;
        var newCount = oldCount + diff;

        newEdge.setDeleted(newCount <= 0);
        if (oldCount != newCount) {
            edgeCountStore.put(edgeCountKey, newCount);
        }
        return newCount;
    }

}
