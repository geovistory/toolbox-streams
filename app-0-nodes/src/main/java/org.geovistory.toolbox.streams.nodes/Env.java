package org.geovistory.toolbox.streams.nodes;

import io.github.cdimascio.dotenv.Dotenv;
import org.geovistory.toolbox.streams.lib.Utils;


public enum Env {
    INSTANCE();


    // CREATE_OUTPUT_FOR_POSTGRES if "true" the app creates
    // a topic "statement_enriched_flat" that can be sinked to postgres
    public final String CREATE_OUTPUT_FOR_POSTGRES;


    Env() {
        // load .env for local development
        Dotenv dotenv = Dotenv
                .configure()
                .ignoreIfMissing()
                .load();
      

        this.CREATE_OUTPUT_FOR_POSTGRES = Utils.coalesce(
                System.getProperty("CREATE_OUTPUT_FOR_POSTGRES"),
                System.getenv("CREATE_OUTPUT_FOR_POSTGRES"),
                dotenv.get("CREATE_OUTPUT_FOR_POSTGRES"),
                "false"
        );
    }
}
