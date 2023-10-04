package org.geovistory.toolbox.streams.entity.processors.community;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.AvroSerdes;
import org.geovistory.toolbox.streams.entity.OutputTopicNames;
import org.geovistory.toolbox.streams.entity.RegisterInputTopic;
import org.geovistory.toolbox.streams.entity.lib.TimeSpanFactory;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedList;


@ApplicationScoped
public class CommunityEntityTimeSpan {


    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;


    @Inject
    OutputTopicNames outputTopicNames;

    @ConfigProperty(name = "ts.community.slug", defaultValue = "")
    private String communitySlug;


    public CommunityEntityTimeSpan(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {
        addProcessors(
                registerInputTopic.communityTopOutgoingStatementsStream()
        );
    }

    public CommunityEntityTimeSpanReturnValue addProcessors(
            KStream<CommunityTopStatementsKey, CommunityTopStatementsValue> communityOutgoingTopStatements
    ) {

        /* STREAM PROCESSORS */
        // 2)

        var n2 = "kstream-flat-map-community-" + communitySlug + "-top-time-primitives";
        var stream = communityOutgoingTopStatements.flatMapValues(
                (key, value) -> {
                    var res = new LinkedList<TopTimePrimitives>();

                    // suppress if not time property
                    if (!TimeSpanFactory.isTimeProperty(key.getPropertyId())) return res;

                    var topTimePrimitives = TopTimePrimitives.newBuilder()
                            .setTimePrimitives(new ArrayList<>())
                            .setPropertyId(key.getPropertyId());

                    // add convert statements to time primitives
                    var array = topTimePrimitives.getTimePrimitives();
                    for (var s : value.getStatements()) {
                        var tp = extractTimePrimitive(s);
                        if (tp != null) array.add(tp);
                    }

                    res.add(topTimePrimitives.build());

                    return res;
                },
                Named.as(n2)
        );

        // 3
        var n3 = "community-" + communitySlug + "project_top_time_primitives_grouped";
        var grouped = stream.groupBy(
                (key, value) -> CommunityEntityKey.newBuilder()
                        .setEntityId(key.getEntityId())
                        .build(),
                Grouped.with(
                        n3,
                        avroSerdes.CommunityEntityKey(),
                        avroSerdes.TopTimePrimitives()
                )
        );


        // 4
        var n4 = "community-" + communitySlug + "top_time_primitives_aggregated";
        var aggregated = grouped.aggregate(
                () -> TopTimePrimitivesMap.newBuilder().build(),
                (key, value, aggregate) -> {
                    aggregate.getMap().put("" + value.getPropertyId(), value);
                    return aggregate;
                },
                Materialized.<CommunityEntityKey, TopTimePrimitivesMap, KeyValueStore<Bytes, byte[]>>as(n4)
                        .withKeySerde(avroSerdes.CommunityEntityKey())
                        .withValueSerde(avroSerdes.TopTimePrimitivesMap())
        );

        // 5
        var n5 = "community-" + communitySlug + "top_time_primitives_aggregated";

        var timeSpanStream = aggregated
                .toStream(
                        Named.as("to-stream-" + n5)
                )
                .mapValues((readOnlyKey, value) -> TimeSpanFactory.createTimeSpan(value));


        /* SINK PROCESSORS */
        timeSpanStream.to(outputTopicNames.communityEntityTimeSpan(),
                Produced.with(avroSerdes.CommunityEntityKey(), avroSerdes.TimeSpanValue())
                        .withName(outputTopicNames.communityEntityTimeSpan() + "-producer")
        );

        return new CommunityEntityTimeSpanReturnValue(timeSpanStream);

    }


    public static TimePrimitive extractTimePrimitive(CommunityStatementValue value) {
        var a = value.getStatement();
        if (a == null) return null;

        var b = a.getObject();
        if (b == null) return null;

        return b.getTimePrimitive();
    }


}
