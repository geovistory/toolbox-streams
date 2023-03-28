package org.geovistory.toolbox.streams.statement.object;

import io.github.cdimascio.dotenv.Dotenv;
import org.geovistory.toolbox.streams.lib.Utils;


public enum Env {
    INSTANCE();

    // Name of topic NODES
    public final String TOPIC_NODES;

    // Name of topic STATEMENT_WITH_SUBJECT
    public final String TOPIC_STATEMENT_WITH_SUBJECT;

    // CREATE_OUTPUT_FOR_POSTGRES if "true" the app creates
    // a topic "statement_enriched_flat" that can be sinked to postgres
    public final String CREATE_OUTPUT_FOR_POSTGRES;


    Env() {
        // load .env for local development
        Dotenv dotenv = Dotenv
                .configure()
                .ignoreIfMissing()
                .load();
        this.TOPIC_NODES = Utils.coalesce(
                System.getProperty("TS_TOPIC_NODES"),
                System.getenv("TS_TOPIC_NODES"),
                dotenv.get("TS_TOPIC_NODES"),
                "TS_TOPIC_NODES"
        );

        this.TOPIC_STATEMENT_WITH_SUBJECT = Utils.coalesce(
                System.getProperty("TS_TOPIC_STATEMENT_WITH_SUBJECT"),
                System.getenv("TS_TOPIC_STATEMENT_WITH_SUBJECT"),
                dotenv.get("TS_TOPIC_STATEMENT_WITH_SUBJECT"),
                "TS_TOPIC_STATEMENT_WITH_SUBJECT"
        );

        this.CREATE_OUTPUT_FOR_POSTGRES = Utils.coalesce(
                System.getProperty("CREATE_OUTPUT_FOR_POSTGRES"),
                System.getenv("CREATE_OUTPUT_FOR_POSTGRES"),
                dotenv.get("CREATE_OUTPUT_FOR_POSTGRES"),
                "false"
        );
    }
}
