package org.geovistory.toolbox.streams.analysis.statements;

import io.github.cdimascio.dotenv.Dotenv;
import org.geovistory.toolbox.streams.lib.Utils;


public enum Env {
    INSTANCE();
    // load .env for local development
    final Dotenv dotenv = Dotenv
            .configure()
            .ignoreIfMissing()
            .load();
    public final String TOPIC_PROJECT_STATEMENT_WITH_LITERAL = parseEnv(
            "TS_TOPIC_PROJECT_STATEMENT_WITH_LITERAL",
            "TS_TOPIC_PROJECT_STATEMENT_WITH_LITERAL");
    public final String TOPIC_PROJECT_STATEMENT_WITH_ENTITY = parseEnv(
            "TS_TOPIC_PROJECT_STATEMENT_WITH_ENTITY",
            "TS_TOPIC_PROJECT_STATEMENT_WITH_ENTITY");

    private String parseEnv(String envVar, String defaultVal) {
        return Utils.coalesce(
                System.getProperty(envVar),
                System.getenv(envVar),
                dotenv.get(envVar),
                defaultVal
        );
    }
}
