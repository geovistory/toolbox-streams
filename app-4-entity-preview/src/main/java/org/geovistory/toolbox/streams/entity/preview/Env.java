package org.geovistory.toolbox.streams.entity.preview;

import io.github.cdimascio.dotenv.Dotenv;
import org.geovistory.toolbox.streams.lib.Utils;


public enum Env {
    INSTANCE();
    // load .env for local development
    final Dotenv dotenv = Dotenv
            .configure()
            .ignoreIfMissing()
            .load();
    // Name of topic PROJECT_ENTITY
    public final String TOPIC_PROJECT_ENTITY = parseEnv(
            "TS_TOPIC_PROJECT_ENTITY",
            "TS_TOPIC_PROJECT_ENTITY");

    // Name of topic PROJECT_ENTITY_LABEL
    public final String TOPIC_PROJECT_ENTITY_LABEL = parseEnv(
            "TS_TOPIC_PROJECT_ENTITY_LABEL",
            "TS_TOPIC_PROJECT_ENTITY_LABEL");

    // Name of topic PROJECT_ENTITY_CLASS_LABEL
    public final String TOPIC_PROJECT_ENTITY_CLASS_LABEL = parseEnv(
            "TS_TOPIC_PROJECT_ENTITY_CLASS_LABEL",
            "TS_TOPIC_PROJECT_ENTITY_CLASS_LABEL");

    // Name of topic PROJECT_ENTITY_TYPE
    public final String TOPIC_PROJECT_ENTITY_TYPE = parseEnv(
            "TS_TOPIC_PROJECT_ENTITY_TYPE",
            "TS_TOPIC_PROJECT_ENTITY_TYPE");

    // Name of topic PROJECT_ENTITY_TIME_SPAN
    public final String TOPIC_PROJECT_ENTITY_TIME_SPAN = parseEnv(
            "TS_TOPIC_PROJECT_ENTITY_TIME_SPAN",
            "TS_TOPIC_PROJECT_ENTITY_TIME_SPAN");

    // Name of topic PROJECT_ENTITY_FULLTEXT
    public final String TOPIC_PROJECT_ENTITY_FULLTEXT = parseEnv(
            "TS_TOPIC_PROJECT_ENTITY_FULLTEXT",
            "TS_TOPIC_PROJECT_ENTITY_FULLTEXT");

    // Name of topic PROJECT_ENTITY_CLASS_METADATA
    public final String TOPIC_PROJECT_ENTITY_CLASS_METADATA = parseEnv(
            "TS_TOPIC_PROJECT_ENTITY_CLASS_METADATA",
            "TS_TOPIC_PROJECT_ENTITY_CLASS_METADATA");



    public final String TOPIC_COMMUNITY_ENTITY = parseEnv(
            "TS_TOPIC_COMMUNITY_ENTITY",
            "TS_TOPIC_COMMUNITY_ENTITY");

    // Name of topic COMMUNITY_ENTITY_LABEL
    public final String TOPIC_COMMUNITY_ENTITY_LABEL = parseEnv(
            "TS_TOPIC_COMMUNITY_ENTITY_LABEL",
            "TS_TOPIC_COMMUNITY_ENTITY_LABEL");

    // Name of topic COMMUNITY_ENTITY_CLASS_LABEL
    public final String TOPIC_COMMUNITY_ENTITY_CLASS_LABEL = parseEnv(
            "TS_TOPIC_COMMUNITY_ENTITY_CLASS_LABEL",
            "TS_TOPIC_COMMUNITY_ENTITY_CLASS_LABEL");

    // Name of topic COMMUNITY_ENTITY_TYPE
    public final String TOPIC_COMMUNITY_ENTITY_TYPE = parseEnv(
            "TS_TOPIC_COMMUNITY_ENTITY_TYPE",
            "TS_TOPIC_COMMUNITY_ENTITY_TYPE");

    // Name of topic COMMUNITY_ENTITY_TIME_SPAN
    public final String TOPIC_COMMUNITY_ENTITY_TIME_SPAN = parseEnv(
            "TS_TOPIC_COMMUNITY_ENTITY_TIME_SPAN",
            "TS_TOPIC_COMMUNITY_ENTITY_TIME_SPAN");

    // Name of topic COMMUNITY_ENTITY_FULLTEXT
    public final String TOPIC_COMMUNITY_ENTITY_FULLTEXT = parseEnv(
            "TS_TOPIC_COMMUNITY_ENTITY_FULLTEXT",
            "TS_TOPIC_COMMUNITY_ENTITY_FULLTEXT");

    // Name of topic COMMUNITY_ENTITY_CLASS_METADATA
    public final String TOPIC_COMMUNITY_ENTITY_CLASS_METADATA = parseEnv(
            "TS_TOPIC_COMMUNITY_ENTITY_CLASS_METADATA",
            "TS_TOPIC_COMMUNITY_ENTITY_CLASS_METADATA");

    private String parseEnv(String envVar, String defaultVal) {
        return Utils.coalesce(
                System.getProperty(envVar),
                System.getenv(envVar),
                dotenv.get(envVar),
                defaultVal
        );
    }
}
