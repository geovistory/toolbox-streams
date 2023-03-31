package org.geovistory.toolbox.streams.statement.subject;

import org.geovistory.toolbox.streams.lib.Utils;

public enum DbTopicNames {

    inf_statement("information.statement");
    private final String name;

    DbTopicNames(String name) {
        this.name = Utils.dbPrefixed(name);
    }

    public String getName() {
        return name;
    }

}
