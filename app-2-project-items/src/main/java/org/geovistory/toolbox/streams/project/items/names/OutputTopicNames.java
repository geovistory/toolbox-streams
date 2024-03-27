package org.geovistory.toolbox.streams.project.items.names;

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

    public final String projectStatementBySub() {
        return p("project_statement_repartitioned_by_subject");
    }

    public final String projectStatementByOb() {
        return p("project_statement_repartitioned_by_object");
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

    public final String projectStatementWithSubByPk() {
        return p("project_statement_with_sub_by_pk");
    }

    public final String projectStatementWithObByPk() {
        return p("project_statement_with_ob_by_pk");
    }

    public final String projectEdgesToolbox() {
        return p("project_edges_toolbox");
    }

    public final String projectEdgesPublic() {
        return p("project_edges_public");
    }

    public final String communityEdgesToolbox() {
        return p("community_edges_toolbox");
    }

    public final String communityEdgesPublic() {
        return p("community_edges_public");
    }

    private String p(String n) {
        return Utils.prefixedOut(outPrefix, n);
    }

}
