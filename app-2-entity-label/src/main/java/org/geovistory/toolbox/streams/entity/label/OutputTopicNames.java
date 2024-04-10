package org.geovistory.toolbox.streams.entity.label;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.lib.Utils;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class OutputTopicNames {
    @ConfigProperty(name = "ts.output.topic.name.prefix", defaultValue = "ts")
    public String outPrefix;

    public final String projectEntityVisibility() {
        return p("project_entity_visibility");
    }

    public final String communityToolboxEntity() {
        return p("community_toolbox_entity");
    }

    public final String communityToolboxEntityLabel() {
        return p("community_toolbox_entity_label");
    }

    public final String communityToolboxEntityWithLabelConfig() {
        return p("community_toolbox_entity_with_label_config");
    }

    public final String communityToolboxStatementWithEntity() {
        return p("community_toolbox_statement_with_entity");
    }

    public final String communityToolboxStatementWithLiteral() {
        return p("community_toolbox_statement_with_literal");
    }

    public final String communityToolboxTopIncomingStatements() {
        return p("community_toolbox_top_incoming_statements");
    }

    public final String communityToolboxTopOutgoingStatements() {
        return p("community_toolbox_top_outgoing_statements");
    }

    public final String communityToolboxTopStatements() {
        return p("community_toolbox_top_statements");
    }

    public final String projectEntity() {
        return p("project_entity");
    }

    public final String projectEntityLabel() {
        return p("project_entity_label");
    }

    public final String projectEntityWithLabelConfig() {
        return p("project_entity_with_label_config");
    }

    public final String projectStatementWithEntity() {
        return p("project_statement_with_entity");
    }

    public final String projectStatementWithLiteral() {
        return p("project_statement_with_literal");
    }

    public final String projectTopIncomingStatements() {
        return p("project_top_incoming_statements");
    }

    public final String projectTopOutgoingStatements() {
        return p("project_top_outgoing_statements");
    }

    public final String projectTopStatements() {
        return p("project_top_statements");
    }

    private String p(String n) {
        return Utils.prefixedOut(outPrefix, n);
    }

}
