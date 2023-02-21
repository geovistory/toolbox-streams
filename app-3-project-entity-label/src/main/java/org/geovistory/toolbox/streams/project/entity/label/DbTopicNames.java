package org.geovistory.toolbox.streams.project.entity.label;

import org.geovistory.toolbox.streams.lib.Utils;

public enum DbTopicNames {
    inf_resource("information.resource"),

    pro_info_proj_rel("projects.info_proj_rel");

    private final String name;

    DbTopicNames(String name) {
        this.name = Utils.dbPrefixed(name);
    }

    public String getName() {
        return name;
    }

}
