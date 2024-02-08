package org.geovistory.toolbox.streams.entity.label;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.lib.TopicNameEnum;
import org.geovistory.toolbox.streams.lib.Utils;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class InputTopicNames {
    @ConfigProperty(name = "ts.input.topic.name.prefix", defaultValue = "ts")
    String prefix;

    @ConfigProperty(name = "ts.topic.statement.with.entity", defaultValue = "")
    String statementWithEntity = "statement.with.entity";
    @ConfigProperty(name = "ts.topic.statement.with.literal", defaultValue = "")
    String statementWithLiteral = "statement.with.literal";
    @ConfigProperty(name = "ts.topic.project.entity.label.config", defaultValue = "")
    String projectEntityLabelConfig = "project.entity.label.config";
    @ConfigProperty(name = "ts.topic.community.entity.label.config", defaultValue = "")
    String communityEntityLabelConfig = "community.entity.label.config";

    public String getStatementWithEntity() {
        return statementWithEntity;
    }

    public String getStatementWithLiteral() {
        return statementWithLiteral;
    }

    public String getProjectEntityLabelConfig() {
        return projectEntityLabelConfig;
    }

    public String getCommunityEntityLabelConfig() {
        return communityEntityLabelConfig;
    }

    public String proInfoProjRel() {
        return Utils.prefixedIn(prefix, TopicNameEnum.pro_info_proj_rel.getValue());
    }

    public String infResource() {
        return Utils.prefixedIn(prefix, TopicNameEnum.inf_resource.getValue());
    }

}
