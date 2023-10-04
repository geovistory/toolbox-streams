package org.geovistory.toolbox.streams.entity.label.processsors.community;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.CommunityTopStatementsKey;
import org.geovistory.toolbox.streams.avro.CommunityTopStatementsValue;
import org.geovistory.toolbox.streams.entity.label.AvroSerdes;
import org.geovistory.toolbox.streams.entity.label.OutputTopicNames;
import org.geovistory.toolbox.streams.entity.label.RegisterInnerTopic;
import org.geovistory.toolbox.streams.entity.label.RegisterInputTopic;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;


@ApplicationScoped
public class CommunityToolboxTopStatements {

    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;
    @Inject
    RegisterInnerTopic registerInnerTopic;

    @Inject
    OutputTopicNames outputTopicNames;

    public CommunityToolboxTopStatements(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, RegisterInnerTopic registerInnerTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.registerInnerTopic = registerInnerTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {

        addProcessors(
                registerInnerTopic.communityToolboxTopOutgoingStatementsStream(),
                registerInnerTopic.communityToolboxTopIncomingStatementsStream()
        );
    }

    public CommunityTopStatementsReturnValue addProcessors(
            KStream<CommunityTopStatementsKey, CommunityTopStatementsValue> outgoingTopStatementsStream,
            KStream<CommunityTopStatementsKey, CommunityTopStatementsValue> incomingTopStatementsStream
    ) {



        /* STREAM PROCESSORS */
        // 2)
        var topStatementsStream = outgoingTopStatementsStream.merge(
                incomingTopStatementsStream,
                Named.as("kstream-merge-community-toolbox-top-out-s-and-community-toolbox-top-in-s")
        );

        /* SINK PROCESSORS */

        topStatementsStream.to(outputTopicNames.communityToolboxTopStatements(),
                Produced.with(avroSerdes.CommunityTopStatementsKey(), avroSerdes.CommunityTopStatementsValue())
                        .withName(outputTopicNames.communityToolboxTopStatements() + "-producer")
        );

        var topStatementsTable = topStatementsStream.toTable(
                Named.as(outputTopicNames.communityToolboxTopStatements()),
                Materialized
                        .<CommunityTopStatementsKey, CommunityTopStatementsValue, KeyValueStore<Bytes, byte[]>>
                                as(outputTopicNames.communityToolboxTopStatements() + "-store")
                        .withKeySerde(avroSerdes.CommunityTopStatementsKey())
                        .withValueSerde(avroSerdes.CommunityTopStatementsValue())
        );


        return new CommunityTopStatementsReturnValue(topStatementsTable, topStatementsStream);

    }


}
