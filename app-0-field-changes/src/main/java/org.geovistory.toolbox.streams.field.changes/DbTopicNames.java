package org.geovistory.toolbox.streams.field.changes;

public enum DbTopicNames {
    inf_statement("information.statement"),
    pro_info_proj_rel("projects.info_proj_rel");

    private final String value;

    DbTopicNames(String value) {
        this.value = value;
    }

    public String getValue(){
        return value;
    }
}
