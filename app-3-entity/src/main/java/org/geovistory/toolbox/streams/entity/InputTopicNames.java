package org.geovistory.toolbox.streams.entity;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class InputTopicNames {

    @ConfigProperty(name = "ts.topic.ontome.class.metadata", defaultValue = "")
    public String ontomeClassMetadata = "ontome.class.metadata";
    @ConfigProperty(name = "ts.topic.has.type.property", defaultValue = "")
    public String hasTypeProperty = "has.type.property";
    @ConfigProperty(name = "ts.topic.project.entity", defaultValue = "")
    public String projectEntity = "project.entity";
    @ConfigProperty(name = "ts.topic.project.edges", defaultValue = "")
    public String projectEdges = "project.edges";
    @ConfigProperty(name = "ts.topic.project.class.label", defaultValue = "")
    public String projectClassLabel = "project.class.label";
    @ConfigProperty(name = "ts.topic.community.entity", defaultValue = "")
    public String communityEntity = "community.entity";
    @ConfigProperty(name = "ts.topic.community.edges", defaultValue = "")
    public String communityEdges = "community.edges";

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

    public String getProjectEdges() {
        return projectEdges;
    }

    public String getProjectClassLabel() {
        return projectClassLabel;
    }

    public String getCommunityEntity() {
        return communityEntity;
    }

    public String getCommunityEdges() {
        return communityEdges;
    }

    public String getCommunityClassLabel() {
        return communityClassLabel;
    }
}
