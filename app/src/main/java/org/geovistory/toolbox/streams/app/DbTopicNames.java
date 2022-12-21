package org.geovistory.toolbox.streams.app;

import org.geovistory.toolbox.streams.lib.Utils;

public enum DbTopicNames {
    pro_projects("projects.project"),
    pro_text_property("projects.text_property"),
    pro_dfh_profile_proj_rel("dfh_profile_proj_rel"),
    sys_config("system.config"),
    dfh_api_property("data_for_history.api_property"),
    dfh_api_class("data_for_history.api_class");

    private final String name;

    DbTopicNames(String name) {
        this.name = Utils.dbPrefixed(name);
    }

    public String getName() {
        return name;
    }

}
