package org.geovistory.toolbox.streams.entity.preview;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.lib.Utils;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class OutputTopicNames {
    @ConfigProperty(name = "ts.output.topic.name.prefix", defaultValue = "")
    public String outPrefix;

    @ConfigProperty(name = "ts.community.slug", defaultValue = "")
    public String communitySlug;

    public final String projectEntityPreview() {
        return p("project_entity_preview");
    }

    public final String entityPreview() {
        return p("entity_preview");
    }

    public final String communityEntityPreview() {
        return p("community_" + communitySlug + "_entity_preview");
    }

    private String p(String n) {
        return Utils.prefixedOut(outPrefix, n);
    }

}
