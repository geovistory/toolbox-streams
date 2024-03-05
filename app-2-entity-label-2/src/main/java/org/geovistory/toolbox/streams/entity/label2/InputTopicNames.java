package org.geovistory.toolbox.streams.entity.label2;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.lib.TopicNameEnum;
import org.geovistory.toolbox.streams.lib.Utils;

@ApplicationScoped
public class InputTopicNames {
    @ConfigProperty(name = "ts.input.topic.name.prefix", defaultValue = "ts")
    String prefix;

    public String infResource() {
        return Utils.prefixedIn(prefix, TopicNameEnum.inf_resource.getValue());
    }

}
