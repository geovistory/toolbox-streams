package org.geovistory.toolbox.streams.entity.label;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.lib.TopicNameEnum;
import org.geovistory.toolbox.streams.lib.Utils;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class InputTopicNames {
    @ConfigProperty(name = "ts.input.topic.name.prefix", defaultValue = "")
    private String prefix;

    @ConfigProperty(name = "ts.topic.statement.with.entity", defaultValue = "")
    public String statementWithEntity = "statement.with.entity";
    @ConfigProperty(name = "ts.topic.statement.with.literal", defaultValue = "")
    public String statementWithLiteral = "statement.with.literal";
    @ConfigProperty(name = "ts.topic.project.entity.label.config", defaultValue = "")
    public String projectEntityLabelConfig = "project.entity.label.config";
    @ConfigProperty(name = "ts.topic.community.entity.label.config", defaultValue = "")
    public String communityEntityLabelConfig = "community.entity.label.config";


    public String proInfoProjRel() {
        return Utils.prefixedIn(prefix, TopicNameEnum.pro_info_proj_rel.getValue());
    }
    public String infResource() {
        return Utils.prefixedIn(prefix, TopicNameEnum.inf_resource.getValue());
    }

}
