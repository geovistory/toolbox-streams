package org.geovistory.toolbox.streams.entity;

import io.github.cdimascio.dotenv.Dotenv;
import org.geovistory.toolbox.streams.lib.Utils;


public enum Env {
    INSTANCE();
    private final Dotenv dotenv = Dotenv
            .configure()
            .ignoreIfMissing()
            .load();

    // Name of topic ONTOME_CLASS_METADATA
    public final String TOPIC_ONTOME_CLASS_METADATA = parseEnv(
            "TS_TOPIC_ONTOME_CLASS_METADATA",
            "TS_TOPIC_ONTOME_CLASS_METADATA");

    // Name of topic HAS_TYPE_PROPERTY
    public final String TOPIC_HAS_TYPE_PROPERTY = parseEnv(
            "TS_TOPIC_HAS_TYPE_PROPERTY",
            "TS_TOPIC_HAS_TYPE_PROPERTY");

    // Name of topic PROJECT_ENTITY_LABEL_CONFIG
    public final String TOPIC_PROJECT_ENTITY_LABEL_CONFIG = parseEnv(
            "TS_TOPIC_PROJECT_ENTITY_LABEL_CONFIG",
            "TS_TOPIC_PROJECT_ENTITY_LABEL_CONFIG");

    // Name of topic PROJECT_TOP_STATEMENTS
    public final String TOPIC_PROJECT_TOP_STATEMENTS = parseEnv(
            "TS_TOPIC_PROJECT_TOP_STATEMENTS",
            "TS_TOPIC_PROJECT_TOP_STATEMENTS");

    // Name of topic PROJECT_ENTITY_TABLE
    public final String TOPIC_PROJECT_ENTITY = parseEnv(
            "TS_TOPIC_PROJECT_ENTITY",
            "TS_TOPIC_PROJECT_ENTITY");

    // Name of topic PROJECT_TOP_OUTGOING_STATEMENTS
    public final String TOPIC_PROJECT_TOP_OUTGOING_STATEMENTS = parseEnv(
            "TS_TOPIC_PROJECT_TOP_OUTGOING_STATEMENTS",
            "TS_TOPIC_PROJECT_TOP_OUTGOING_STATEMENTS");

    // Name of topic PROJECT_CLASS_LABEL
    public final String TOPIC_PROJECT_CLASS_LABEL = parseEnv(
            "TS_TOPIC_PROJECT_CLASS_LABEL",
            "TS_TOPIC_PROJECT_CLASS_LABEL");

    // Name of topic project_property_label
    public final String TOPIC_PROJECT_PROPERTY_LABEL = parseEnv(
            "TS_TOPIC_PROJECT_PROPERTY_LABEL",
            "TS_TOPIC_PROJECT_PROPERTY_LABEL");





    // Name of topic COMMUNITY_ENTITY_LABEL_CONFIG
    public final String TOPIC_COMMUNITY_ENTITY_LABEL_CONFIG = parseEnv(
            "TS_TOPIC_COMMUNITY_ENTITY_LABEL_CONFIG",
            "TS_TOPIC_COMMUNITY_ENTITY_LABEL_CONFIG");

    // Name of topic COMMUNITY_TOP_STATEMENTS
    public final String TOPIC_COMMUNITY_TOP_STATEMENTS = parseEnv(
            "TS_TOPIC_COMMUNITY_TOP_STATEMENTS",
            "TS_TOPIC_COMMUNITY_TOP_STATEMENTS");

    // Name of topic COMMUNITY_ENTITY_TABLE
    public final String TOPIC_COMMUNITY_ENTITY = parseEnv(
            "TS_TOPIC_COMMUNITY_ENTITY",
            "TS_TOPIC_COMMUNITY_ENTITY");

    // Name of topic COMMUNITY_TOP_OUTGOING_STATEMENTS
    public final String TOPIC_COMMUNITY_TOP_OUTGOING_STATEMENTS = parseEnv(
            "TS_TOPIC_COMMUNITY_TOP_OUTGOING_STATEMENTS",
            "TS_TOPIC_COMMUNITY_TOP_OUTGOING_STATEMENTS");

    // Name of topic COMMUNITY_CLASS_LABEL
    public final String TOPIC_COMMUNITY_CLASS_LABEL = parseEnv(
            "TS_TOPIC_COMMUNITY_CLASS_LABEL",
            "TS_TOPIC_COMMUNITY_CLASS_LABEL");

    // Name of topic COMMUNITY_PROPERTY_LABEL
    public final String TOPIC_COMMUNITY_PROPERTY_LABEL = parseEnv(
            "TS_TOPIC_COMMUNITY_PROPERTY_LABEL",
            "TS_TOPIC_COMMUNITY_PROPERTY_LABEL");


    private String parseEnv(String envVar, String defaultVal) {
        return Utils.coalesce(
                System.getProperty(envVar),
                System.getenv(envVar),
                dotenv.get(envVar),
                defaultVal
        );
    }

}
