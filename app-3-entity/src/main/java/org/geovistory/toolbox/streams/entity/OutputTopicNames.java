package org.geovistory.toolbox.streams.entity;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.lib.Utils;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class OutputTopicNames {
    @ConfigProperty(name = "ts.output.topic.name.prefix", defaultValue = "")
    public String outPrefix;

    @ConfigProperty(name = "ts.community.slug", defaultValue = "")
    public String communitySlug;

    public final String projectEntityClassLabel() {
        return p("project_entity_class_label");
    }

    public final String projectEntityTimeSpan() {
        return p("project_entity_time_span");
    }

    public final String projectEntityClassMetadata() {
        return p("project_entity_class_metadata");
    }

    public final String projectEntityType() {
        return p("project_entity_type");
    }

    public final String communityEntityClassLabel() {
        return p("community_" + communitySlug + "_entity_class_label");
    }

    public final String communityEntityTimeSpan() {
        return p("community_" + communitySlug + "_entity_time_span");
    }

    public final String communityEntityClassMetadata() {
        return p("community_" + communitySlug + "_entity_class_metadata");
    }

    public final String communityEntityType() {
        return p("community_" + communitySlug + "_entity_type");
    }


    private String p(String n) {
        return Utils.prefixedOut(outPrefix, n);
    }

}
