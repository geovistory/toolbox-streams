package org.geovistory.toolbox.streams.project.config;

import org.geovistory.toolbox.streams.lib.Utils;

public enum DbTopicNames {

    pro_projects("projects.project"),
    pro_text_property("projects.text_property"),
    pro_entity_label_config("projects.entity_label_config"),
    pro_dfh_profile_proj_rel("projects.dfh_profile_proj_rel"),
    sys_config("system.config");

    private final String name;

    DbTopicNames(String name) {
        this.name = Utils.dbPrefixed(name);
    }

    public String getName() {
        return name;
    }

}
