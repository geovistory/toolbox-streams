package org.geovistory.toolbox.streams.entity.label3.stores;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.geovistory.toolbox.streams.entity.label3.names.PubTargets;
import org.geovistory.toolbox.streams.lib.processorapi.AbstractStore;

/**
 * Store for storing if an entity labels is published on a publication target
 * <p>
 * with key: {project-id}_{entity-id}_{pub-target} for entity labels
 * {project-id}_{entity-id}_{lang}_{pub-target} for entity language labels
 * <p>
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

    public static String createKey(PubTargets pubTarget, String projectId, String entityId) {
        return String.join("_", new String[]{projectId, entityId, pubTarget.name()});
    }

    public static String createKey(PubTargets pubTarget, String projectId, String entityId, String language) {
        return String.join("_", new String[]{projectId, entityId, language, pubTarget.name()});
    }


}
