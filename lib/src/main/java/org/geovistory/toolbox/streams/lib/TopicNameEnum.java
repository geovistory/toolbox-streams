package org.geovistory.toolbox.streams.lib;

public enum TopicNameEnum {
    inf_statement("information.statement"),
    pro_info_proj_rel("projects.info_proj_rel");

    private final String value;

    TopicNameEnum(String value) {
        this.value = value;
    }

    public String getValue(){
        return value;
    }
}
