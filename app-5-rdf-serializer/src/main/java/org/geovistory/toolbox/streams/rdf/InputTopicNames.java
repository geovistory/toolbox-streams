package org.geovistory.toolbox.streams.rdf;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class InputTopicNames {

    @ConfigProperty(name = "ts.topic.project.statement.with.entity", defaultValue = "")
    public String projectStatementWithEntity = "ts.topic.project.statement.with.entity";
    @ConfigProperty(name = "ts.topic.project.statement.with.literal", defaultValue = "")
    public String projectStatementWithLiteral = "ts.topic.project.statement.with.literal";

    public String getProjectStatementWithEntity() {
        return projectStatementWithEntity;
    }

    public String getProjectStatementWithLiteral() {
        return projectStatementWithLiteral;
    }
}
