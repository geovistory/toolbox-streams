package org.geovistory.toolbox.streams.fulltext;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class InputTopicNames {
    @ConfigProperty(name = "ts.topic.project.entity.with.label.config", defaultValue = "")
    private final String projectEntityWithLabelConfig = "project.entity.with.label.config";
    @ConfigProperty(name = "ts.topic.project.top.statements", defaultValue = "")
    private final String projectTopStatements = "project.top.statements";
    @ConfigProperty(name = "ts.topic.project.property.label", defaultValue = "")
    private final String projectPropertyLabel = "project.property.label";
    @ConfigProperty(name = "ts.topic.community.entity.with.label.config", defaultValue = "")
    private final String communityEntityWithLabelConfig = "community.entity.with.label.config";
    @ConfigProperty(name = "ts.topic.community.top.statements", defaultValue = "")
    private final String communityTopStatements = "community.top.statements";
    @ConfigProperty(name = "ts.topic.community.property.label", defaultValue = "")
    private final String communityPropertyLabel = "community.property.label";

    public String getProjectEntityWithLabelConfig() {
        return projectEntityWithLabelConfig;
    }

    public String getProjectTopStatements() {
        return projectTopStatements;
    }

    public String getProjectPropertyLabel() {
        return projectPropertyLabel;
    }

    public String getCommunityEntityWithLabelConfig() {
        return communityEntityWithLabelConfig;
    }

    public String getCommunityTopStatements() {
        return communityTopStatements;
    }

    public String getCommunityPropertyLabel() {
        return communityPropertyLabel;
    }
}
