package org.geovistory.toolbox.streams.lib;

public enum TopicNameEnum {

    pro_info_proj_rel("projects.info_proj_rel"),
    dfh_api_property("data_for_history.api_property"),
    dfh_api_class("data_for_history.api_class"),
    inf_statement("information.statement"),
    inf_resource("information.resource"),
    inf_language("information.language"),
    inf_appellation("information.appellation"),
    inf_lang_string("information.lang_string"),
    inf_place("information.place"),
    inf_time_primitive("information.time_primitive"),
    inf_dimension("information.dimension"),
    dat_digital("data.digital"),
    tab_cell("tables.cell"),
    pro_projects("projects.project"),
    pro_text_property("projects.text_property"),
    pro_entity_label_config("projects.entity_label_config"),
    pro_dfh_profile_proj_rel("projects.dfh_profile_proj_rel"),
    sys_config("system.config");

    private final String value;

    TopicNameEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
