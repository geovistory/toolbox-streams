package org.geovistory.toolbox.streams.field.changes;

import org.geovistory.toolbox.streams.lib.Utils;

public enum DbTopicNames {
    inf_statement("information.statement"),
    pro_info_proj_rel("projects.info_proj_rel");
    private final String name;

    DbTopicNames(String name) {
        this.name = Utils.dbPrefixed(name);
    }

    public String getName() {
        return name;
    }

}
