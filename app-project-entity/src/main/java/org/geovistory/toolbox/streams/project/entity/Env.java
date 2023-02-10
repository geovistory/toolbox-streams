package org.geovistory.toolbox.streams.project.entity;

import io.github.cdimascio.dotenv.Dotenv;
import org.geovistory.toolbox.streams.lib.Utils;


public enum Env {
    INSTANCE();

    // Name of topic PROJECT_ENTITY_LABEL_CONFIG
    public final String TOPIC_PROJECT_ENTITY_LABEL_CONFIG;

    // Name of topic PROJECT_ENTITY_TOP_STATEMENTS
    public final String TOPIC_PROJECT_ENTITY_TOP_STATEMENTS;

    // Name of topic PROJECT_ENTITY_TABLE
    public final String TOPIC_PROJECT_ENTITY;

    // Name of topic PROJECT_TOP_OUTGOING_STATEMENTS
    public final String TOPIC_PROJECT_TOP_OUTGOING_STATEMENTS;

    // Name of topic ONTOME_CLASS_METADATA
    public final String TOPIC_ONTOME_CLASS_METADATA;

    // Name of topic PROJECT_CLASS_LABEL
    public final String TOPIC_PROJECT_CLASS_LABEL;

    // Name of topic HAS_TYPE_PROPERTY
    public final String TOPIC_HAS_TYPE_PROPERTY;

    Env() {
        // load .env for local development
        Dotenv dotenv = Dotenv
                .configure()
                .ignoreIfMissing()
                .load();

        this.TOPIC_PROJECT_ENTITY_LABEL_CONFIG = Utils.coalesce(
                System.getProperty("TS_TOPIC_PROJECT_ENTITY_LABEL_CONFIG"),
                System.getenv("TS_TOPIC_PROJECT_ENTITY_LABEL_CONFIG"),
                dotenv.get("TS_TOPIC_PROJECT_ENTITY_LABEL_CONFIG"),
                "TS_TOPIC_PROJECT_ENTITY_LABEL_CONFIG"
        );
        this.TOPIC_PROJECT_ENTITY_TOP_STATEMENTS = Utils.coalesce(
                System.getProperty("TS_TOPIC_PROJECT_ENTITY_TOP_STATEMENTS"),
                System.getenv("TS_TOPIC_PROJECT_ENTITY_TOP_STATEMENTS"),
                dotenv.get("TS_TOPIC_PROJECT_ENTITY_TOP_STATEMENTS"),
                "TS_TOPIC_PROJECT_ENTITY_TOP_STATEMENTS"
        );
        this.TOPIC_PROJECT_ENTITY = Utils.coalesce(
                System.getProperty("TS_TOPIC_PROJECT_ENTITY"),
                System.getenv("TS_TOPIC_PROJECT_ENTITY"),
                dotenv.get("TS_TOPIC_PROJECT_ENTITY"),
                "TS_TOPIC_PROJECT_ENTITY"
        );
        this.TOPIC_PROJECT_TOP_OUTGOING_STATEMENTS = Utils.coalesce(
                System.getProperty("TS_TOPIC_PROJECT_TOP_OUTGOING_STATEMENTS"),
                System.getenv("TS_TOPIC_PROJECT_TOP_OUTGOING_STATEMENTS"),
                dotenv.get("TS_TOPIC_PROJECT_TOP_OUTGOING_STATEMENTS"),
                "TS_TOPIC_PROJECT_TOP_OUTGOING_STATEMENTS"
        );
        this.TOPIC_ONTOME_CLASS_METADATA = Utils.coalesce(
                System.getProperty("TS_TOPIC_ONTOME_CLASS_METADATA"),
                System.getenv("TS_TOPIC_ONTOME_CLASS_METADATA"),
                dotenv.get("TS_TOPIC_ONTOME_CLASS_METADATA"),
                "TS_TOPIC_ONTOME_CLASS_METADATA"
        );
        this.TOPIC_PROJECT_CLASS_LABEL = Utils.coalesce(
                System.getProperty("TS_TOPIC_PROJECT_CLASS_LABEL"),
                System.getenv("TS_TOPIC_PROJECT_CLASS_LABEL"),
                dotenv.get("TS_TOPIC_PROJECT_CLASS_LABEL"),
                "TS_TOPIC_PROJECT_CLASS_LABEL"
        );
        TOPIC_HAS_TYPE_PROPERTY = Utils.coalesce(
                System.getProperty("TS_TOPIC_HAS_TYPE_PROPERTY"),
                System.getenv("TS_TOPIC_HAS_TYPE_PROPERTY"),
                dotenv.get("TS_TOPIC_HAS_TYPE_PROPERTY"),
                "TS_TOPIC_HAS_TYPE_PROPERTY"
        );

    }
}
