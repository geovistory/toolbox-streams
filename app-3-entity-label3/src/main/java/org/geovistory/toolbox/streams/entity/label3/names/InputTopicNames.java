package org.geovistory.toolbox.streams.entity.label3.names;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.lib.TopicNameEnum;
import org.geovistory.toolbox.streams.lib.Utils;

@ApplicationScoped
public class InputTopicNames {
    @ConfigProperty(name = "ts.input.topic.name.prefix", defaultValue = "ts")
    String prefix;

    @ConfigProperty(name = "ts.topic.project.edges", defaultValue = "")
    String projectEdges = "project.edges";

    public String getProjectEdges() {
        return projectEdges;
    }

    public String proEntityLabelConfig() {
        return Utils.prefixedIn(prefix, TopicNameEnum.pro_entity_label_config.getValue());
    }

}
