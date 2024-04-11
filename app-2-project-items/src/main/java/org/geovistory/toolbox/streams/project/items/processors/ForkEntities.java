package org.geovistory.toolbox.streams.project.items.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.EntityValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectEntityValue;
import org.geovistory.toolbox.streams.project.items.lib.ExtractVisiblity;
import org.geovistory.toolbox.streams.project.items.names.SinkNames;
import org.geovistory.toolbox.streams.project.items.stores.EntityBoolStore;
import org.geovistory.toolbox.streams.project.items.stores.EntityIntStore;

import static org.geovistory.toolbox.streams.project.items.lib.Fn.getOperation;

public class ForkEntities implements Processor<ProjectEntityKey, EntityValue, ProjectEntityKey, ProjectEntityValue> {
    private KeyValueStore<String, Integer> intStore;
    private KeyValueStore<String, Boolean> boolStore;
    private ProcessorContext<ProjectEntityKey, ProjectEntityValue> context;

    @Override
    public void init(ProcessorContext<ProjectEntityKey, ProjectEntityValue> context) {
        intStore = context.getStateStore(EntityIntStore.NAME);
        boolStore = context.getStateStore(EntityBoolStore.NAME);
        this.context = context;
    }

    @Override
    public void process(Record<ProjectEntityKey, EntityValue> record) {
        var v = record.value();
        if (v == null) return;
        var k = record.key();
        var classKey = EntityIntStore.createClassKey(k.getEntityId());
        var countKey = EntityIntStore.createNumberKey(k.getEntityId());
        var oldCount = intStore.get(countKey);
        var oldClass = intStore.get(classKey);
        oldClass = oldClass == null ? -1 : oldClass;
        if (oldClass != v.getClassId()) {
            // store new class
            intStore.put(classKey, v.getClassId());
        }

        var projectEntityId = EntityBoolStore.createProjectEntityKey(k);
        var wasPresent = boolStore.get(projectEntityId);
        wasPresent = wasPresent != null && wasPresent;

        int newCount = 1;
        // if deleted
        if (wasPresent && v.getDeleted()) {
            // decrease count
            newCount = oldCount == null ? 0 : oldCount - 1;
            // mark as not present
            boolStore.put(projectEntityId, false);
        } else if (!wasPresent && !v.getDeleted()) {
            // increase count
            newCount = oldCount == null ? 1 : oldCount + 1;
            // mark as present
            boolStore.put(projectEntityId, true);
        }
        // store new count
        intStore.put(countKey, newCount);

        var communityKey = ProjectEntityKey.newBuilder(k).setProjectId(0).build();
        var communityRecord = record.withKey(communityKey);
        // if in 0 projects, the entity is deleted for the community
        var deletedForCommunity = newCount == 0;
        var modified = v.getClassId() != oldClass;

        // forward to toolbox community entity topic
        forward(
                communityRecord,
                EntityValue::getCommunityVisibilityToolbox,
                EntityBoolStore.createTargetKey("ent_tc", communityKey),
                modified,
                deletedForCommunity,
                SinkNames.TOOLBOX_COMMUNITY_ENTITY_SINK);


        // forward to public community entity topic
        forward(
                communityRecord,
                EntityValue::getCommunityVisibilityDataApi,
                EntityBoolStore.createTargetKey("ent_pc", communityKey),
                modified,
                deletedForCommunity,
                SinkNames.PUBLIC_COMMUNITY_ENTITY_SINK);

        // forward to public project entity topic
        forward(
                record,
                EntityValue::getProjectVisibilityDataApi,
                EntityBoolStore.createTargetKey("ent_pp", k),
                modified,
                v.getDeleted(),
                SinkNames.PUBLIC_PROJECT_ENTITY_SINK);

        // forward to toolbox project entity topic
        forward(
                record,
                (e) -> true,
                EntityBoolStore.createTargetKey("ent_tp", k),
                modified,
                v.getDeleted(),
                SinkNames.TOOLBOX_PROJECT_ENTITY_SINK);

    }


    public void forward(
            Record<ProjectEntityKey, EntityValue> record,
            ExtractVisiblity<EntityValue> visibility,
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
            case DELETE ->
                    this.context.forward(record.withValue(getProjectEntityValue(record.value(), true)), childName);
            case INSERT, UPDATE ->
                    this.context.forward(record.withValue(getProjectEntityValue(record.value(), false)), childName);
        }

    }


    private static ProjectEntityValue getProjectEntityValue(EntityValue v, boolean deleted) {
        return ProjectEntityValue.newBuilder()
                .setEntityId(v.getEntityId())
                .setProjectId(v.getProjectId())
                .setClassId(v.getClassId())
                .setDeleted$1(deleted).build();
    }
}


