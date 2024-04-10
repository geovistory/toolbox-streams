package org.geovistory.toolbox.streams.base.model;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.lib.Utils;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class OutputTopicNames {
    @ConfigProperty(name = "ts.output.topic.name.prefix", defaultValue = "ts")
    public String outPrefix;

    public final String ontomeProperty() {
        return p("ontome_property");
    }

    public final String ontomeClass() {
        return p("ontome_class");
    }
    public final String ontomeClassMetadata() {
        return p("ontome_class_metadata");
    }
    public final String hasTypeProperty() {
        return p("has_type_property");
    }
    public final String ontomeClassLabel() {
        return p("ontome_class_label");
    }
    public final String ontomePropertyLabel() {
        return p("ontome_property_label");
    }



    private String p(String n) {
        return Utils.prefixedOut(outPrefix, n);
    }

}
