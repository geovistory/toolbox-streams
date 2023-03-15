package org.geovistory.toolbox.streams.fulltext;

import io.github.cdimascio.dotenv.Dotenv;
import org.geovistory.toolbox.streams.lib.Utils;


public enum Env {
    INSTANCE();
    private final Dotenv dotenv = Dotenv
            .configure()
            .ignoreIfMissing()
            .load();

    // Name of topic PROJECT_ENTITY_LABEL_CONFIG
    public final String TOPIC_PROJECT_ENTITY_WITH_LABEL_CONFIG = parseEnv(
            "TS_TOPIC_PROJECT_ENTITY_WITH_LABEL_CONFIG",
            "TS_TOPIC_PROJECT_ENTITY_WITH_LABEL_CONFIG");

    // Name of topic PROJECT_TOP_STATEMENTS
    public final String TOPIC_PROJECT_TOP_STATEMENTS = parseEnv(
            "TS_TOPIC_PROJECT_TOP_STATEMENTS",
            "TS_TOPIC_PROJECT_TOP_STATEMENTS");



    // Name of topic project_property_label
    public final String TOPIC_PROJECT_PROPERTY_LABEL = parseEnv(
            "TS_TOPIC_PROJECT_PROPERTY_LABEL",
            "TS_TOPIC_PROJECT_PROPERTY_LABEL");





    // Name of topic COMMUNITY_ENTITY_LABEL_CONFIG
    public final String TOPIC_COMMUNITY_ENTITY_WITH_LABEL_CONFIG = parseEnv(
            "TS_TOPIC_COMMUNITY_ENTITY_WITH_LABEL_CONFIG",
            "TS_TOPIC_COMMUNITY_ENTITY_WITH_LABEL_CONFIG");

    // Name of topic COMMUNITY_TOP_STATEMENTS
    public final String TOPIC_COMMUNITY_TOP_STATEMENTS = parseEnv(
            "TS_TOPIC_COMMUNITY_TOP_STATEMENTS",
            "TS_TOPIC_COMMUNITY_TOP_STATEMENTS");

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
