package org.geovistory.toolbox.streams.base.model;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.lib.TopicNameEnum;
import org.geovistory.toolbox.streams.lib.Utils;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class InputTopicNames {

    @ConfigProperty(name = "ts.input.topic.name.prefix", defaultValue = "")
    String prefix;
    public String dfhApiProperty() {
        return Utils.prefixedIn(prefix, TopicNameEnum.dfh_api_property.getValue());
    }

    public String dfhApiClass() {
        return Utils.prefixedIn(prefix, TopicNameEnum.dfh_api_class.getValue());
    }
}
