package org.geovistory.toolbox.streams.rdf;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

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
    @ConfigProperty(name = "ts.topic.project.entity", defaultValue = "")
    public String projectEntity = "ts.topic.project.entity";
    @ConfigProperty(name = "ts.topic.ontome.property.label", defaultValue = "")
    public String ontomePropertyLabel = "ontome.property.label";

    @ConfigProperty(name = "ts.topic.projects.project", defaultValue = "")
    public String project = "ts.topic.projects.project";

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

    public String getProjectEntity() {
        return projectEntity;
    }

    public String getProject() {
        return project;
    }

    public String getOntomePropertyLabel() {
        return ontomePropertyLabel;
    }
}
