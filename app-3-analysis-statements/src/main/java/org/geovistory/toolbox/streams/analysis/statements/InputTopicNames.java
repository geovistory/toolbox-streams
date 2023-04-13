package org.geovistory.toolbox.streams.analysis.statements;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class InputTopicNames {
    @ConfigProperty(name = "ts.input.topic.name.prefix", defaultValue = "")
    private String prefix;


    @ConfigProperty(name = "ts.topic.project.statement.with.literal", defaultValue = "")
    private final String projectStatementWithLiteral = "project.statement.with.literal";
    @ConfigProperty(name = "ts.topic.project.statement.with.entity", defaultValue = "")
    private final String projectStatementWithEntity = "project.statement.with.entity";

    public String getProjectStatementWithLiteral() {
        return projectStatementWithLiteral;
    }

    public String getProjectStatementWithEntity() {
        return projectStatementWithEntity;
    }
}
