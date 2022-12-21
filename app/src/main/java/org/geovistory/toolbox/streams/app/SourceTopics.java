package org.geovistory.toolbox.streams.app;

import org.geovistory.toolbox.streams.lib.Utils;

public enum SourceTopics {
    api_class("data_for_history.api_class");
    private final String name;

    SourceTopics(String name) {
        this.name = Utils.dbPrefixed(name);
    }

    public String getName() {
        return name;
    }

}
