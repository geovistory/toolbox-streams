package org.geovistory.toolbox.streams.fulltext;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.lib.Utils;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class OutputTopicNames {
    @ConfigProperty(name = "ts.output.topic.name.prefix", defaultValue = "")
    public String outPrefix;

    @ConfigProperty(name = "ts.community.slug", defaultValue = "")
    public String communitySlug;

    public final String projectEntityFulltext() {
        return p("project_entity_fulltext");
    }

    public final String communityEntityFulltext() {
        return p("community_" + communitySlug + "_entity_fulltext");
    }

    private String p(String n) {
        return Utils.prefixedOut(outPrefix, n);
    }

}
