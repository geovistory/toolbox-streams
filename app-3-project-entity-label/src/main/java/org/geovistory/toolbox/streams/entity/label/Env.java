package org.geovistory.toolbox.streams.entity.label;

import io.github.cdimascio.dotenv.Dotenv;
import org.geovistory.toolbox.streams.lib.Utils;


public enum Env {
    INSTANCE();

    // Name of topic statement_with_entity
    public final String TOPIC_STATEMENT_WITH_ENTITY;

    // Name of topic statement_with_literal
    public final String TOPIC_STATEMENT_WITH_LITERAL;

    // Name of topic project_entity_label_config
    public final String TOPIC_PROJECT_ENTITY_LABEL_CONFIG;



    Env() {
        // load .env for local development
        Dotenv dotenv = Dotenv
                .configure()
                .ignoreIfMissing()
                .load();

        this.TOPIC_STATEMENT_WITH_ENTITY = Utils.coalesce(
                System.getProperty("TS_TOPIC_STATEMENT_WITH_ENTITY"),
                System.getenv("TS_TOPIC_STATEMENT_WITH_ENTITY"),
                dotenv.get("TS_TOPIC_STATEMENT_WITH_ENTITY"),
                "TS_TOPIC_STATEMENT_WITH_ENTITY"
        );
        this.TOPIC_STATEMENT_WITH_LITERAL = Utils.coalesce(
                System.getProperty("TS_TOPIC_STATEMENT_WITH_LITERAL"),
                System.getenv("TS_TOPIC_STATEMENT_WITH_LITERAL"),
                dotenv.get("TS_TOPIC_STATEMENT_WITH_LITERAL"),
                "TS_TOPIC_STATEMENT_WITH_LITERAL"
        );
        this.TOPIC_PROJECT_ENTITY_LABEL_CONFIG = Utils.coalesce(
                System.getProperty("TS_TOPIC_PROJECT_ENTITY_LABEL_CONFIG"),
                System.getenv("TS_TOPIC_PROJECT_ENTITY_LABEL_CONFIG"),
                dotenv.get("TS_TOPIC_PROJECT_ENTITY_LABEL_CONFIG"),
                "TS_TOPIC_PROJECT_ENTITY_LABEL_CONFIG"
        );
    }
}
