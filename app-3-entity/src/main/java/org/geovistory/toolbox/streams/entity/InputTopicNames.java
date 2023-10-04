package org.geovistory.toolbox.streams.entity;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class InputTopicNames {

    @ConfigProperty(name = "ts.topic.ontome.class.metadata", defaultValue = "")
    public String ontomeClassMetadata = "ontome.class.metadata";
    @ConfigProperty(name = "ts.topic.has.type.property", defaultValue = "")
    public String hasTypeProperty = "has.type.property";
    @ConfigProperty(name = "ts.topic.project.entity", defaultValue = "")
    public String projectEntity = "project.entity";
    @ConfigProperty(name = "ts.topic.project.top.outgoing.statements", defaultValue = "")
    public String projectTopOutgoingStatements = "project.top.outgoing.statements";
    @ConfigProperty(name = "ts.topic.project.class.label", defaultValue = "")
    public String projectClassLabel = "project.class.label";
    @ConfigProperty(name = "ts.topic.community.entity", defaultValue = "")
    public String communityEntity = "community.entity";
    @ConfigProperty(name = "ts.topic.community.top.outgoing.statements", defaultValue = "")
    public String communityTopOutgoingStatements = "community.top.outgoing.statements";

    @ConfigProperty(name = "ts.topic.community.class.label", defaultValue = "")
    public String communityClassLabel = "community.class.label";

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
