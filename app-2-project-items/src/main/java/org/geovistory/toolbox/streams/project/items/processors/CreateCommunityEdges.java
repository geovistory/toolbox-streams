package org.geovistory.toolbox.streams.project.items.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.EdgeValue;
import org.geovistory.toolbox.streams.avro.InProjectOrdNum;
import org.geovistory.toolbox.streams.project.items.stores.EdgeCountStore;
import org.geovistory.toolbox.streams.project.items.stores.EdgeOrdNumStore;
import org.geovistory.toolbox.streams.project.items.stores.EdgeSumStore;


public class CreateCommunityEdges implements Processor<String, EdgeValue, String, EdgeValue> {

    private ProcessorContext<String, EdgeValue> context;
    private KeyValueStore<String, Integer> edgeCountStore;
    private KeyValueStore<String, Float> edgeSumStore;
    private KeyValueStore<String, InProjectOrdNum> edgeOrdNumStore;

    private final String slug;

    public CreateCommunityEdges(String slug) {
        this.slug = slug;
    }

    @Override
    public void init(ProcessorContext<String, EdgeValue> context) {
        edgeCountStore = context.getStateStore(EdgeCountStore.NAME);
        edgeOrdNumStore = context.getStateStore(EdgeOrdNumStore.NAME);
        edgeSumStore = context.getStateStore(EdgeSumStore.NAME);
        this.context = context;
    }

    public void process(Record<String, EdgeValue> record) {
        var newEdge = EdgeValue.newBuilder(record.value()).build();
        var edgeCountKey = EdgeCountStore.createKey(slug, newEdge);
        var edgeSumKey = EdgeSumStore.createKey(slug, newEdge);

        var edgeBoolKey = EdgeOrdNumStore.createKey(slug, newEdge);
        var newIsInProject = !newEdge.getDeleted();
        var oldInProjectOrdNum = edgeOrdNumStore.get(edgeBoolKey);
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
        var newAvgOrdNum = createAverageOrdNum(newEdge, oldOrnNum, edgeSumKey, removed, newCount);


        edgeOrdNumStore.put(edgeBoolKey,
                InProjectOrdNum.newBuilder().setInProject(newIsInProject)
                        .setOrdNum(newAvgOrdNum)
                        .build()
        );

        newEdge.setProjectId(0);
        newEdge.setSourceProjectEntity(null);
        newEdge.setTargetProjectEntity(null);
        context.forward(record.withKey(edgeCountKey).withValue(newEdge));
    }

    private Float createAverageOrdNum(
            EdgeValue newEdge,
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

    private int createCount(EdgeValue newEdge, String edgeCountKey, boolean added, boolean removed) {
        var diff = added ? 1 : removed ? -1 : 0;

        Integer oldCount = edgeCountStore.get(edgeCountKey);
        oldCount = oldCount != null ? oldCount : 0;
        var newCount = oldCount + diff;

        newEdge.setDeleted(newCount <= 0);
        if (oldCount != newCount) {
            edgeCountStore.put(edgeCountKey, newCount);
        }
        newEdge.setProjectCount(newCount);
        return newCount;
    }


}
