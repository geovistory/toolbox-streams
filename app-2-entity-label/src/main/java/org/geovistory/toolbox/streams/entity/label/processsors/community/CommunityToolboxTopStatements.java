package org.geovistory.toolbox.streams.entity.label.processsors.community;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.CommunityTopStatementsKey;
import org.geovistory.toolbox.streams.avro.CommunityTopStatementsValue;
import org.geovistory.toolbox.streams.entity.label.RegisterInnerTopic;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;


public class CommunityToolboxTopStatements {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerOutputTopic = new RegisterInnerTopic(builder);

        return addProcessors(
                builder,
                registerOutputTopic.communityToolboxTopOutgoingStatementsStream(),
                registerOutputTopic.communityToolboxTopIncomingStatementsStream()
        ).builder().build();
    }

    public static CommunityTopStatementsReturnValue addProcessors(
            StreamsBuilder builder,
            KStream<CommunityTopStatementsKey, CommunityTopStatementsValue> outgoingTopStatementsStream,
            KStream<CommunityTopStatementsKey, CommunityTopStatementsValue> incomingTopStatementsStream
    ) {

        var avroSerdes = new ConfluentAvroSerdes();


        /* STREAM PROCESSORS */
        // 2)
        var topStatementsStream = outgoingTopStatementsStream.merge(
                incomingTopStatementsStream,
                Named.as("kstream-merge-community-toolbox-top-out-s-and-community-toolbox-top-in-s")
        );

        /* SINK PROCESSORS */

        topStatementsStream.to(output.TOPICS.community_toolbox_top_statements,
                Produced.with(avroSerdes.CommunityTopStatementsKey(), avroSerdes.CommunityTopStatementsValue())
                        .withName(output.TOPICS.community_toolbox_top_statements + "-producer")
        );

        var topStatementsTable = topStatementsStream.toTable(
                Named.as(output.TOPICS.community_toolbox_top_statements),
                Materialized
                        .<CommunityTopStatementsKey, CommunityTopStatementsValue, KeyValueStore<Bytes, byte[]>>
                                as(output.TOPICS.community_toolbox_top_statements + "-store")
                        .withKeySerde(avroSerdes.CommunityTopStatementsKey())
                        .withValueSerde(avroSerdes.CommunityTopStatementsValue())
        );


        return new CommunityTopStatementsReturnValue(builder, topStatementsTable, topStatementsStream);

    }


    public enum input {
        TOPICS;
        public final String community_toolbox_top_outgoing_statements = CommunityToolboxTopOutgoingStatements.output.TOPICS.community_toolbox_top_outgoing_statements;
        public final String community_toolbox_top_incoming_statements = CommunityToolboxTopIncomingStatements.output.TOPICS.community_toolbox_top_incoming_statements;
    }


    public enum output {
        TOPICS;
        public final String community_toolbox_top_statements = Utils.tsPrefixed("community_toolbox_top_statements");
    }

}
