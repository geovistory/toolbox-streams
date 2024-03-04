package org.geovistory.toolbox.streams.base.config;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.lib.TopicNameEnum;
import org.geovistory.toolbox.streams.lib.Utils;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class InputTopicNames {
    @ConfigProperty(name = "ts.input.topic.name.prefix", defaultValue = "ts")
    String prefix;

    @ConfigProperty(name = "ts.topic.ontome.class", defaultValue = "")
    public String topicOntomeClass = "ontome.class";
    @ConfigProperty(name = "ts.topic.ontome.property", defaultValue = "")
    public String topicOntomeProperty = "ontome.property";
    @ConfigProperty(name = "ts.topic.ontome.class.label", defaultValue = "")
    public String topicOntomeClassLabel = "ontome.class.label";
    @ConfigProperty(name = "ts.topic.ontome.property.label", defaultValue = "")
    public String topicOntomePropertyLabel = "ontome.property.label";


    public String proTextProperty() {
        return Utils.prefixedIn(prefix, TopicNameEnum.pro_text_property.getValue());
    }

    public String proProfileProjRel() {
        return Utils.prefixedIn(prefix, TopicNameEnum.pro_dfh_profile_proj_rel.getValue());
    }

    public String proProject() {
        return Utils.prefixedIn(prefix, TopicNameEnum.pro_projects.getValue());
    }

    public String sysConfig() {
        return Utils.prefixedIn(prefix, TopicNameEnum.sys_config.getValue());
    }

    public String proEntityLabelConfig() {
        return Utils.prefixedIn(prefix, TopicNameEnum.pro_entity_label_config.getValue());
    }

    public String ontomeClass() {
        return topicOntomeClass;
    }

    public String ontomeProperty() {
        return topicOntomeProperty;
    }

    public String ontomeClassLabel() {
        return topicOntomeClassLabel;
    }


    public String ontomePropertyLabel() {
        return topicOntomePropertyLabel;
    }

}
