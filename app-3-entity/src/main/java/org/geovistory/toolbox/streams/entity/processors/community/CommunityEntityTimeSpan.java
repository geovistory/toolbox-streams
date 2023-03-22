package org.geovistory.toolbox.streams.entity.processors.community;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.RegisterInputTopic;
import org.geovistory.toolbox.streams.entity.lib.TimeSpanFactory;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.ArrayList;
import java.util.LinkedList;


public class CommunityEntityTimeSpan {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder(), "toolbox").describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder, String nameSupplement) {

        var inputTopic = new RegisterInputTopic(builder);

        var communityEntityTopStatementsValueStream = inputTopic.communityTopOutgoingStatementsStream();

        return addProcessors(builder,
                communityEntityTopStatementsValueStream,
                nameSupplement
        ).builder().build();
    }

    public static CommunityEntityTimeSpanReturnValue addProcessors(
            StreamsBuilder builder,
            KStream<CommunityTopStatementsKey, CommunityTopStatementsValue> communityOutgoingTopStatements,
            String nameSupplement
    ) {

        var avroSerdes = new ConfluentAvroSerdes();

        /* STREAM PROCESSORS */
        // 2)

        var n2 = "kstream-flat-map-community-" + nameSupplement + "-top-time-primitives";
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
        var n3 = "community-" + nameSupplement + "project_top_time_primitives_grouped";
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
        var n4 = "community-" + nameSupplement + "top_time_primitives_aggregated";
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
        var n5 = "community-" + nameSupplement + "top_time_primitives_aggregated";

        var timeSpanStream = aggregated
                .toStream(
                        Named.as("to-stream-" + n5)
                )
                .mapValues((readOnlyKey, value) -> TimeSpanFactory.createTimeSpan(value));


        /* SINK PROCESSORS */
        timeSpanStream.to(getOutputTopicName(nameSupplement),
                Produced.with(avroSerdes.CommunityEntityKey(), avroSerdes.TimeSpanValue())
                        .withName(getOutputTopicName(nameSupplement) + "-producer")
        );

        return new CommunityEntityTimeSpanReturnValue(builder, timeSpanStream);

    }


    public static String getOutputTopicName(String nameSupplement) {
        return Utils.tsPrefixed("community_" + nameSupplement + "_entity_time_span");
    }

    public static TimePrimitive extractTimePrimitive(CommunityStatementValue value) {
        var a = value.getStatement();
        if (a == null) return null;

        var b = a.getObject();
        if (b == null) return null;

        return b.getTimePrimitive();
    }


}
