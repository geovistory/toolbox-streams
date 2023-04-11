package org.geovistory.toolbox.streams.base.config;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.lib.Utils;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class OutputTopicNames {
    @ConfigProperty(name = "ts.output.topic.name.prefix", defaultValue = "")
    public String outPrefix;

    public final String projectProfile() {
        return p("project_profile");
    }

    public final String geovClassLabel() {
        return p("geov_class_label");
    }

    public final String projectClass() {
        return p("project_class");
    }

    public final String geovPropertyLabel() {
        return p("geov_property_label");
    }

    public final String projectProperty() {
        return p("project_property");
    }

    public final String communityEntityLabelConfig() {
        return p("community_entity_label_config");
    }

    public String communityClassLabel() {
        return p("community_class_label");
    }

    public final String communityPropertyLabel() {
        return p("community_property_label");
    }

    public final String projectClassLabel() {
        return p("project_class_label");
    }

    public final String projectEntityLabelConfig() {
        return p("project_entity_label_config");
    }

    public final String projectPropertyLabel() {
        return p("project_property_label");
    }


    private String p(String n) {
        return Utils.prefixedOut(outPrefix, n);
    }

}
