package org.geovistory.toolbox.streams.entity;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class InputTopicNames {

    @ConfigProperty(name = "ts.topic.ontome.class.metadata", defaultValue = "")
    private final String ontomeClassMetadata = "ontome.class.metadata";
    @ConfigProperty(name = "ts.topic.has.type.property", defaultValue = "")
    private final String hasTypeProperty = "has.type.property";
    @ConfigProperty(name = "ts.topic.project.entity", defaultValue = "")
    private final String projectEntity = "project.entity";
    @ConfigProperty(name = "ts.topic.project.top.outgoing.statements", defaultValue = "")
    private final String projectTopOutgoingStatements = "project.top.outgoing.statements";
    @ConfigProperty(name = "ts.topic.project.class.label", defaultValue = "")
    private final String projectClassLabel = "project.class.label";
    @ConfigProperty(name = "ts.topic.community.entity", defaultValue = "")
    private final String communityEntity = "community.entity";
    @ConfigProperty(name = "ts.topic.community.top.outgoing.statements", defaultValue = "")
    private final String communityTopOutgoingStatements = "community.top.outgoing.statements";

    @ConfigProperty(name = "ts.topic.community.class.label", defaultValue = "")
    private final String communityClassLabel = "community.class.label";

    public String getOntomeClassMetadata() {
        return ontomeClassMetadata;
    }

    public String getHasTypeProperty() {
        return hasTypeProperty;
    }

    public String getProjectEntity() {
        return projectEntity;
    }

    public String getProjectTopOutgoingStatements() {
        return projectTopOutgoingStatements;
    }

    public String getProjectClassLabel() {
        return projectClassLabel;
    }

    public String getCommunityEntity() {
        return communityEntity;
    }

    public String getCommunityTopOutgoingStatements() {
        return communityTopOutgoingStatements;
    }

    public String getCommunityClassLabel() {
        return communityClassLabel;
    }
}
