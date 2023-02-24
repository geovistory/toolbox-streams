package org.geovistory.toolbox.streams.entity.processors.community;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.geovistory.toolbox.streams.avro.CommunityEntityKey;
import org.geovistory.toolbox.streams.avro.CommunityEntityTopStatementsValue;
import org.geovistory.toolbox.streams.avro.TimeSpanValue;
import org.geovistory.toolbox.streams.entity.RegisterInnerTopic;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.LinkedList;
import java.util.List;

import static org.geovistory.toolbox.streams.entity.lib.TimeSpanFactory.createTimeSpan;


public class CommunityEntityTimeSpan {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder(), "toolbox").describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder, String nameSupplement) {

        var innerTopic = new RegisterInnerTopic(builder);

        var communityEntityTopStatementsValueStream = innerTopic.communityEntityTopStatementsStream(nameSupplement);

        return addProcessors(builder,
                communityEntityTopStatementsValueStream,
                nameSupplement
        ).builder().build();
    }

    public static CommunityEntityTimeSpanReturnValue addProcessors(
            StreamsBuilder builder,
            KStream<CommunityEntityKey, CommunityEntityTopStatementsValue> projectEntityTopStatementsValueStream,
            String nameSupplement
    ) {

        var avroSerdes = new ConfluentAvroSerdes();


        /* STREAM PROCESSORS */
        // 2)

        var stream = projectEntityTopStatementsValueStream.flatMapValues(
                (readOnlyKey, value) -> {
                    List<TimeSpanValue> result = new LinkedList<>();
                    var tspv = createTimeSpan(value);
                    if (tspv != null) result.add(tspv);
                    return result;
                },
                Named.as("kstream-flat-map-community-" + nameSupplement + "-entity-top-statements-to-time-span")
        );



        /* SINK PROCESSORS */
        stream.to(getOutputTopicName(nameSupplement),
                Produced.with(avroSerdes.CommunityEntityKey(), avroSerdes.TimeSpanValue())
                        .withName(getOutputTopicName(nameSupplement) + "-producer")
        );

        return new CommunityEntityTimeSpanReturnValue(builder, stream);

    }


    public static String getOutputTopicName(String nameSupplement) {
        return Utils.tsPrefixed("community_" + nameSupplement + "_entity_class_metadata");
    }


}
