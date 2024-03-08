package org.geovistory.toolbox.streams.entity.label2.names;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.lib.Utils;

@ApplicationScoped
public class OutputTopicNames {
    @ConfigProperty(name = "ts.output.topic.name.prefix", defaultValue = "ts")
    public String outPrefix;

    public final String projectEntity() {
        return p("project_entity");
    }

    public final String projectStatement() {
        return p("project_statement");
    }

    public final String iprRepartitioned() {
        return p("ipr_repartitioned");
    }

    public final String sRepartitioned() {
        return p("s_repartitioned");
    }

    public final String eRepartitioned() {
        return p("e_repartitioned");
    }

    private String p(String n) {
        return Utils.prefixedOut(outPrefix, n);
    }

}
