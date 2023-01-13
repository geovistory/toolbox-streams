package org.geovistory.toolbox.streams.app;

import org.geovistory.toolbox.streams.lib.Utils;

public enum DbTopicNames {
    inf_resource("information.resource"),
    inf_statement("information.statement"),
    inf_language("information.language"),
    inf_appellation("information.appellation"),
    inf_lang_string("information.lang_string"),
    inf_place("information.place"),
    inf_time_primitive("information.time_primitive"),
    inf_dimension("information.dimension"),
    pro_projects("projects.project"),
    pro_text_property("projects.text_property"),
    pro_info_proj_rel("projects.info_proj_rel"),
    pro_entity_label_config("projects.entity_label_config"),
    pro_dfh_profile_proj_rel("projects.dfh_profile_proj_rel"),
    sys_config("system.config"),
    dfh_api_property("data_for_history.api_property"),
    dfh_api_class("data_for_history.api_class"),
    dat_digital("data.digital"),
    tab_cell("tables.cell");

    private final String name;

    DbTopicNames(String name) {
        this.name = Utils.dbPrefixed(name);
    }

    public String getName() {
        return name;
    }

}
