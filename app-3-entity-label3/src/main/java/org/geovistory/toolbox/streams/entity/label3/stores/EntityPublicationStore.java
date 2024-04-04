package org.geovistory.toolbox.streams.entity.label3.stores;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.geovistory.toolbox.streams.entity.label3.names.PubTargets;
import org.geovistory.toolbox.streams.lib.processorapi.AbstractStore;

/**
 * Store for storing if an entity labels is published on a publication target
 * <p>
 * with key: {project-id}_{entity-id}_{pub-target}
 * with val: true, if published, false or null if not
 *
 * <p>
 * pub-targets:
 * - TC (toolbox-community)
 * - PC (public-community)
 * - PP (public-project)
 * <p>
 * project-id defaults to 0 for community pub-targets
 */
@ApplicationScoped
public class EntityPublicationStore extends AbstractStore<String, Boolean> {
    public static final String NAME = "entity-publication-store";

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

    public static String createKey(PubTargets pubTarget, int projectId, String entityId) {
        return String.join("_", new String[]{Integer.toString(projectId), entityId, pubTarget.name()});
    }

}
