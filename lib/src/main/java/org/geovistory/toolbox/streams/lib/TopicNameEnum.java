package org.geovistory.toolbox.streams.lib;

public enum TopicNameEnum {
    inf_statement("information.statement"),
    pro_info_proj_rel("projects.info_proj_rel"),
    dfh_api_property("data_for_history.property"),
    dfh_api_class("data_for_history.class");

    private final String value;

    TopicNameEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
