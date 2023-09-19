package org.geovistory.toolbox.streams.rdf;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.lib.Utils;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class OutputTopicNames {
    @ConfigProperty(name = "ts.output.topic.name.prefix", defaultValue = "")
    public String outPrefix;

    public final String projectRdf() {
        return p("project_rdf");
    }


    private String p(String n) {
        return Utils.prefixedOut(outPrefix, n);
    }

}
