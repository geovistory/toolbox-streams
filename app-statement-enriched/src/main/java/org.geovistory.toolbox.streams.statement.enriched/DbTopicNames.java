package org.geovistory.toolbox.streams.statement.enriched;

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
