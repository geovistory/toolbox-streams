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

    public final String toolboxProjectEdges() {
        return p("toolbox_project_edges");
    }

    public final String toolboxCommunityEdges() {
        return p("toolbox_community_edges");
    }

    public final String publicProjectEdges() {
        return p("public_project_edges");
    }

    public final String publicCommunityEdges() {
        return p("public_community_edges");
    }

    public final String toolboxProjectEntities() {
        return p("toolbox_project_entities");
    }

    public final String toolboxCommunityEntities() {
        return p("toolbox_community_entities");
    }


    public final String publicProjectEntities() {
        return p("public_project_entities");
    }

    public final String publicCommunityEntities() {
        return p("public_community_entities");
    }

    private String p(String n) {
        return Utils.prefixedOut(outPrefix, n);
    }

}
