package org.geovistory.toolbox.streams.entity.preview;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class InputTopicNames {
    @ConfigProperty(name = "ts.topic.project.entity", defaultValue = "")
    public String projectEntity = "ts_topic_project_entity";
    @ConfigProperty(name = "ts.topic.project.entity.label", defaultValue = "")
    public String projectEntityLabel = "ts_topic_project_entity_label";
    @ConfigProperty(name = "ts.topic.project.entity.type", defaultValue = "")
    public String projectEntityType = "ts_topic_project_entity_type";
    @ConfigProperty(name = "ts.topic.project.entity.time.span", defaultValue = "")
    public String projectEntityTimeSpan = "ts_topic_project_entity_time_span";
    @ConfigProperty(name = "ts.topic.project.entity.fulltext", defaultValue = "")
    public String projectEntityFulltext = "ts_topic_project_entity_fulltext";
    @ConfigProperty(name = "ts.topic.project.entity.class.label", defaultValue = "")
    public String projectEntityClassLabel = "ts_topic_project_entity_class_label";
    @ConfigProperty(name = "ts.topic.project.entity.class.metadata", defaultValue = "")
    public String projectEntityClassMetadata = "ts_topic_project_entity_class_metadata";
    @ConfigProperty(name = "ts.topic.community.entity", defaultValue = "")
    public String communityEntity = "ts_topic_community_entity";
    @ConfigProperty(name = "ts.topic.community.entity.label", defaultValue = "")
    public String communityEntityLabel = "ts_topic_community_entity_label";
    @ConfigProperty(name = "ts.topic.community.entity.type", defaultValue = "")
    public String communityEntityType = "ts_topic_community_entity_type";
    @ConfigProperty(name = "ts.topic.community.entity.time.span", defaultValue = "")
    public String communityEntityTimeSpan = "ts_topic_community_entity_time_span";
    @ConfigProperty(name = "ts.topic.community.entity.fulltext", defaultValue = "")
    public String communityEntityFulltext = "ts_topic_community_entity_fulltext";
    @ConfigProperty(name = "ts.topic.community.entity.class.label", defaultValue = "")
    public String communityEntityClassLabel = "ts_topic_community_entity_class_label";
    @ConfigProperty(name = "ts.topic.community.entity.class.metadata", defaultValue = "")
    public String communityEntityClassMetadata = "ts_topic_community_entity_class_metadata";
}
