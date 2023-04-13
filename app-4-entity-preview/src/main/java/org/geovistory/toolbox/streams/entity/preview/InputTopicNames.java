package org.geovistory.toolbox.streams.entity.preview;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class InputTopicNames {
    @ConfigProperty(name = "ts.topic.project.entity", defaultValue = "")
    private final String projectEntity = "ts_topic_project_entity";
    @ConfigProperty(name = "ts.topic.project.entity.label", defaultValue = "")
    private final String projectEntityLabel = "ts_topic_project_entity_label";
    @ConfigProperty(name = "ts.topic.project.entity.type", defaultValue = "")
    private final String projectEntityType = "ts_topic_project_entity_type";
    @ConfigProperty(name = "ts.topic.project.entity.time.span", defaultValue = "")
    private final String projectEntityTimeSpan = "ts_topic_project_entity_time_span";
    @ConfigProperty(name = "ts.topic.project.entity.fulltext", defaultValue = "")
    private final String projectEntityFulltext = "ts_topic_project_entity_fulltext";
    @ConfigProperty(name = "ts.topic.project.entity.class.label", defaultValue = "")
    private final String projectEntityClassLabel = "ts_topic_project_entity_class_label";
    @ConfigProperty(name = "ts.topic.project.entity.class.metadata", defaultValue = "")
    private final String projectEntityClassMetadata = "ts_topic_project_entity_class_metadata";
    @ConfigProperty(name = "ts.topic.community.entity", defaultValue = "")
    private final String communityEntity = "ts_topic_community_entity";
    @ConfigProperty(name = "ts.topic.community.entity.label", defaultValue = "")
    private final String communityEntityLabel = "ts_topic_community_entity_label";
    @ConfigProperty(name = "ts.topic.community.entity.type", defaultValue = "")
    private final String communityEntityType = "ts_topic_community_entity_type";
    @ConfigProperty(name = "ts.topic.community.entity.time.span", defaultValue = "")
    private final String communityEntityTimeSpan = "ts_topic_community_entity_time_span";
    @ConfigProperty(name = "ts.topic.community.entity.fulltext", defaultValue = "")
    private final String communityEntityFulltext = "ts_topic_community_entity_fulltext";
    @ConfigProperty(name = "ts.topic.community.entity.class.label", defaultValue = "")
    private final String communityEntityClassLabel = "ts_topic_community_entity_class_label";
    @ConfigProperty(name = "ts.topic.community.entity.class.metadata", defaultValue = "")
    private final String communityEntityClassMetadata = "ts_topic_community_entity_class_metadata";

    public String getProjectEntity() {
        return projectEntity;
    }

    public String getProjectEntityLabel() {
        return projectEntityLabel;
    }

    public String getProjectEntityType() {
        return projectEntityType;
    }

    public String getProjectEntityTimeSpan() {
        return projectEntityTimeSpan;
    }

    public String getProjectEntityFulltext() {
        return projectEntityFulltext;
    }

    public String getProjectEntityClassLabel() {
        return projectEntityClassLabel;
    }

    public String getProjectEntityClassMetadata() {
        return projectEntityClassMetadata;
    }

    public String getCommunityEntity() {
        return communityEntity;
    }

    public String getCommunityEntityLabel() {
        return communityEntityLabel;
    }

    public String getCommunityEntityType() {
        return communityEntityType;
    }

    public String getCommunityEntityTimeSpan() {
        return communityEntityTimeSpan;
    }

    public String getCommunityEntityFulltext() {
        return communityEntityFulltext;
    }

    public String getCommunityEntityClassLabel() {
        return communityEntityClassLabel;
    }

    public String getCommunityEntityClassMetadata() {
        return communityEntityClassMetadata;
    }
}
