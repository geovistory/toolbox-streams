package org.geovistory.toolbox.streams.project.items.stores;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.geovistory.toolbox.streams.avro.EdgeVisibilityValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.lib.processorapi.AbstractStore;

/**
 * Store to persist if entity is present
 * in project
 * key: entityId_projectID
 * or in slug
 * key: slug_entityId
 * val: true/false
 * where slug is public or toolbox
 */
@ApplicationScoped
public class EntityBoolStore extends AbstractStore<String, Boolean> {
    public static final String NAME = "entity-bool-store";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Serde<String> getKeySerde() {
        return Serdes.String();
    }

    @Override
    public Serde<Boolean> getValueSerde() {
        return Serdes.Boolean();
    }

    public static String createProjectEntityKey(ProjectEntityKey k) {
        return String.join("_", new String[]{k.getEntityId(), Integer.toString(k.getProjectId())});
    }

    public static String createProjectEdgeKey(EdgeVisibilityValue k) {
        return String.join("_", new String[]{k.getSourceId(), Integer.toString(k.getPropertyId()), k.getIsOutgoing() ? "o" : "i", k.getTargetId(), Integer.toString(k.getProjectId())});
    }

    public static String createTargetKey(String target, ProjectEntityKey k) {
        return target + "_" + createProjectEntityKey(k);
    }

    public static String createTargetKey(String target, EdgeVisibilityValue k) {
        return target + "_" + createProjectEdgeKey(k);
    }
}
