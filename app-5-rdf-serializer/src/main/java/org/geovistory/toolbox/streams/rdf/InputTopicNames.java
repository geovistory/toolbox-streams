package org.geovistory.toolbox.streams.rdf;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class InputTopicNames {

    @ConfigProperty(name = "ts.topic.project.statement.with.entity", defaultValue = "")
    public String projectStatementWithEntity = "ts.topic.project.statement.with.entity";
    @ConfigProperty(name = "ts.topic.project.statement.with.literal", defaultValue = "")
    public String projectStatementWithLiteral = "ts.topic.project.statement.with.literal";
    @ConfigProperty(name = "ts.topic.project.class.label", defaultValue = "")
    public String projectClassLabel = "ts.topic.project.class.label";
    @ConfigProperty(name = "ts.topic.project.entity.label", defaultValue = "")
    public String projectEntityLabel = "ts.topic.project.entity.label";

    public String getProjectStatementWithEntity() {
        return projectStatementWithEntity;
    }

    public String getProjectStatementWithLiteral() {
        return projectStatementWithLiteral;
    }

    public String getProjectClassLabel() {
        return projectClassLabel;
    }

    public String getProjectEntityLabel() {
        return projectEntityLabel;
    }
}
