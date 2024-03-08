package org.geovistory.toolbox.streams.entity.label2.names;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.lib.TopicNameEnum;
import org.geovistory.toolbox.streams.lib.Utils;

@ApplicationScoped
public class InputTopicNames {
    @ConfigProperty(name = "ts.input.topic.name.prefix", defaultValue = "ts")
    String prefix;
    @ConfigProperty(name = "ts.topic.statement.with.literal", defaultValue = "")
    String statementWithLiteral = "statement.with.literal";

    public String getStatementWithLiteral() {
        return statementWithLiteral;
    }

    public String infResource() {
        return Utils.prefixedIn(prefix, TopicNameEnum.inf_resource.getValue());
    }

    public String proInfProjRel() {
        return Utils.prefixedIn(prefix, TopicNameEnum.pro_info_proj_rel.getValue());
    }
}
