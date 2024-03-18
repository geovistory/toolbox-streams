package org.geovistory.toolbox.streams.entity.label3.names;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.lib.Utils;

@ApplicationScoped
public class OutputTopicNames {
    @ConfigProperty(name = "ts.output.topic.name.prefix", defaultValue = "ts")
    public String outPrefix;

    public final String labelEdgeBySource() {
        return p("label_edge_by_source");
    }

    public final String labelEdgeByTarget() {
        return p("label_edge_by_target");
    }

    private String p(String n) {
        return Utils.prefixedOut(outPrefix, n);
    }

}
