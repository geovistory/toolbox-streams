package org.geovistory.toolbox.streams.project.entity.preview;

import io.github.cdimascio.dotenv.Dotenv;
import org.geovistory.toolbox.streams.lib.Utils;


public enum Env {
    INSTANCE();

    // Name of topic PROJECT_ENTITY
    public final String TOPIC_PROJECT_ENTITY;

    // Name of topic PROJECT_ENTITY_LABEL
    public final String TOPIC_PROJECT_ENTITY_LABEL;

    // Name of topic PROJECT_ENTITY_CLASS_LABEL
    public final String TOPIC_PROJECT_ENTITY_CLASS_LABEL;

    // Name of topic PROJECT_ENTITY_TYPE
    public final String TOPIC_PROJECT_ENTITY_TYPE;

    // Name of topic PROJECT_ENTITY_TIME_SPAN
    public final String TOPIC_PROJECT_ENTITY_TIME_SPAN;

    // Name of topic PROJECT_ENTITY_FULLTEXT
    public final String TOPIC_PROJECT_ENTITY_FULLTEXT;

    // Name of topic PROJECT_ENTITY_CLASS_METADATA
    public final String TOPIC_PROJECT_ENTITY_CLASS_METADATA;

    Env() {
        // load .env for local development
        Dotenv dotenv = Dotenv
                .configure()
                .ignoreIfMissing()
                .load();


        TOPIC_PROJECT_ENTITY = Utils.coalesce(
                System.getProperty("TS_TOPIC_PROJECT_ENTITY"),
                System.getenv("TS_TOPIC_PROJECT_ENTITY"),
                dotenv.get("TSTOPIC_PROJECT_ENTITY"),
                "TS_TOPIC_PROJECT_ENTITY"
        );
        TOPIC_PROJECT_ENTITY_LABEL = Utils.coalesce(
                System.getProperty("TS_TOPIC_PROJECT_ENTITY_LABEL"),
                System.getenv("TS_TOPIC_PROJECT_ENTITY_LABEL"),
                dotenv.get("TS_TOPIC_PROJECT_ENTITY_LABEL"),
                "TS_TOPIC_PROJECT_ENTITY_LABEL"
        );
        TOPIC_PROJECT_ENTITY_CLASS_LABEL = Utils.coalesce(
                System.getProperty("TS_TOPIC_PROJECT_ENTITY_CLASS_LABEL"),
                System.getenv("TS_TOPIC_PROJECT_ENTITY_CLASS_LABEL"),
                dotenv.get("TS_TOPIC_PROJECT_ENTITY_CLASS_LABEL"),
                "TS_TOPIC_PROJECT_ENTITY_CLASS_LABEL"
        );
        TOPIC_PROJECT_ENTITY_TYPE = Utils.coalesce(
                System.getProperty("TS_TOPIC_PROJECT_ENTITY_TYPE"),
                System.getenv("TS_TOPIC_PROJECT_ENTITY_TYPE"),
                dotenv.get("TS_TOPIC_PROJECT_ENTITY_TYPE"),
                "TS_TOPIC_PROJECT_ENTITY_TYPE"
        );
        TOPIC_PROJECT_ENTITY_TIME_SPAN = Utils.coalesce(
                System.getProperty("TS_TOPIC_PROJECT_ENTITY_TIME_SPAN"),
                System.getenv("TS_TOPIC_PROJECT_ENTITY_TIME_SPAN"),
                dotenv.get("TS_TOPIC_PROJECT_ENTITY_TIME_SPAN"),
                "TS_TOPIC_PROJECT_ENTITY_TIME_SPAN"
        );
        TOPIC_PROJECT_ENTITY_FULLTEXT = Utils.coalesce(
                System.getProperty("TS_TOPIC_PROJECT_ENTITY_FULLTEXT"),
                System.getenv("TS_TOPIC_PROJECT_ENTITY_FULLTEXT"),
                dotenv.get("TS_TOPIC_PROJECT_ENTITY_FULLTEXT"),
                "TS_TOPIC_PROJECT_ENTITY_FULLTEXT"
        );
        TOPIC_PROJECT_ENTITY_CLASS_METADATA = Utils.coalesce(
                System.getProperty("TS_TOPIC_PROJECT_ENTITY_CLASS_METADATA"),
                System.getenv("TS_TOPIC_PROJECT_ENTITY_CLASS_METADATA"),
                dotenv.get("TS_TOPIC_PROJECT_ENTITY_CLASS_METADATA"),
                "TS_TOPIC_PROJECT_ENTITY_CLASS_METADATA"
        );

    }
}
