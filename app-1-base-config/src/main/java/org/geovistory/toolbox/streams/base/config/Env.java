package org.geovistory.toolbox.streams.base.config;

import io.github.cdimascio.dotenv.Dotenv;
import org.geovistory.toolbox.streams.lib.Utils;


public enum Env {
    INSTANCE();

    // Name of topic ONTOME_PROPERTY
    public final String TOPIC_ONTOME_PROPERTY;

    // Name of topic ONTOME_CLASS
    public final String TOPIC_ONTOME_CLASS;

    // Name of topic ONTOME_PROPERTY_LABEL
    public final String TOPIC_ONTOME_PROPERTY_LABEL;

    // Name of topic ONTOME_CLASS_LABEL
    public final String TOPIC_ONTOME_CLASS_LABEL;



    Env() {
        // load .env for local development
        Dotenv dotenv = Dotenv
                .configure()
                .ignoreIfMissing()
                .load();
        this.TOPIC_ONTOME_PROPERTY = Utils.coalesce(
                System.getProperty("TS_TOPIC_ONTOME_PROPERTY"),
                System.getenv("TS_TOPIC_ONTOME_PROPERTY"),
                dotenv.get("TS_TOPIC_ONTOME_PROPERTY"),
                "TS_TOPIC_ONTOME_PROPERTY"
        );
        this.TOPIC_ONTOME_CLASS = Utils.coalesce(
                System.getProperty("TS_TOPIC_ONTOME_CLASS"),
                System.getenv("TS_TOPIC_ONTOME_CLASS"),
                dotenv.get("TS_TOPIC_ONTOME_CLASS"),
                "TS_TOPIC_ONTOME_CLASS"
        );
        this.TOPIC_ONTOME_PROPERTY_LABEL = Utils.coalesce(
                System.getProperty("TS_TOPIC_ONTOME_PROPERTY_LABEL"),
                System.getenv("TS_TOPIC_ONTOME_PROPERTY_LABEL"),
                dotenv.get("TS_TOPIC_ONTOME_PROPERTY_LABEL"),
                "TS_TOPIC_ONTOME_PROPERTY_LABEL"
        );
        this.TOPIC_ONTOME_CLASS_LABEL = Utils.coalesce(
                System.getProperty("TS_TOPIC_ONTOME_CLASS_LABEL"),
                System.getenv("TS_TOPIC_ONTOME_CLASS_LABEL"),
                dotenv.get("TS_TOPIC_ONTOME_CLASS_LABEL"),
                "TS_TOPIC_ONTOME_CLASS_LABEL"
        );

    }
}
