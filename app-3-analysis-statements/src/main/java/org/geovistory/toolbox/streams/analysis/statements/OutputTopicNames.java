package org.geovistory.toolbox.streams.analysis.statements;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.lib.Utils;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class OutputTopicNames {
    @ConfigProperty(name = "ts.output.topic.name.prefix", defaultValue = "ts")
    public String outPrefix;

    public final String projectAnalysisStatement() {
        return p("project_analysis_statement");
    }

    private String p(String n) {
        return Utils.prefixedOut(outPrefix, n);
    }

}
