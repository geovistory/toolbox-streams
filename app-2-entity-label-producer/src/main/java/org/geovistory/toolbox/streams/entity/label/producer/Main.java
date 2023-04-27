package org.acme.kafka.streams.producer.generator;

import dev.projects.info_proj_rel.Key;
import dev.projects.info_proj_rel.Value;
import io.smallrye.reactive.messaging.kafka.Record;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.Random;

/**
 * A bean producing random temperature data every second.
 * The values are written to a Kafka topic (temperature-values).
 * Another topic contains the name of weather stations (weather-stations).
 * The Kafka configuration is specified in the application configuration.
 */
@ApplicationScoped
public class Main {

    private static final Logger LOG = Logger.getLogger(Main.class);

    private final Random random = new Random();

    private final List<Record<Key, Value>> proInfoProjRel = List.of(
            Record.of(
                    Key.newBuilder().setFkEntity(1).setFkProject(1).build(),
                    Value.newBuilder().setFkEntity(1).setFkProject(1).setIsInProject(true).build()
            )
    );

  /*  @Outgoing("pro-info-proj-rel")
    public Multi<Record<Key, Value>> generate() {
        return Multi.createFrom().ticks().every(Duration.ofMillis(500))
                .onOverflow().drop()
                .map(tick -> {
                    var r = proInfoProjRel.get(random.nextInt(proInfoProjRel.size()));
                    LOG.info("pro-info-proj-rel " + r.key().toString());
                    return r;
                });
    }*/

}
