package org.geovistory.toolbox.streams.base.model;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.lib.Utils;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class OutputTopicNames {
    @ConfigProperty(name = "ts.output.topic.name.prefix", defaultValue = "")
    public String outPrefix;

    public final String ontomeProperty() {
        return p("ontome_property");
    }

    public final String ontomeClass() {
        return p("ontome_class");
    }


    private String p(String n) {
        return Utils.prefixedOut(outPrefix, n);
    }

}
