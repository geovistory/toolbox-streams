package org.geovistory.toolbox.streams.base.model;

import org.geovistory.toolbox.streams.lib.Utils;

public enum DbTopicNames {
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
