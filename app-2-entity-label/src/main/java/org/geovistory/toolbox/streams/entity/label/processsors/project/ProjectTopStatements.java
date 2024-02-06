package org.geovistory.toolbox.streams.entity.label.processsors.project;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.ProjectTopStatementsKey;
import org.geovistory.toolbox.streams.avro.ProjectTopStatementsValue;
import org.geovistory.toolbox.streams.entity.label.AvroSerdes;
import org.geovistory.toolbox.streams.entity.label.OutputTopicNames;
import org.geovistory.toolbox.streams.entity.label.RegisterInnerTopic;
import org.geovistory.toolbox.streams.entity.label.RegisterInputTopic;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;


@ApplicationScoped
public class ProjectTopStatements {

    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;
    @Inject
    RegisterInnerTopic registerInnerTopic;

    @Inject
    OutputTopicNames outputTopicNames;

    public ProjectTopStatements(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, RegisterInnerTopic registerInnerTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.registerInnerTopic = registerInnerTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {


        addProcessors(
                registerInnerTopic.projectTopOutgoingStatementsStream(),
                registerInnerTopic.projectTopIncomingStatementsStream()
        );
    }

    public ProjectTopStatementsReturnValue addProcessors(
            KStream<ProjectTopStatementsKey, ProjectTopStatementsValue> outgoingTopStatementsStream,
            KStream<ProjectTopStatementsKey, ProjectTopStatementsValue> incomingTopStatementsStream
    ) {

        /* STREAM PROCESSORS */
        // 2)
        var topStatementsStream = outgoingTopStatementsStream.merge(
                incomingTopStatementsStream,
                Named.as("kstream-merge-project-top-out-s-and-project-top-in-s")
        );

        /* SINK PROCESSORS */

        topStatementsStream.to(outputTopicNames.projectTopStatements(),
                Produced.with(avroSerdes.ProjectTopStatementsKey(), avroSerdes.ProjectTopStatementsValue())
                        .withName(outputTopicNames.projectTopStatements() + "-producer")
        );

        var topStatementsTable = topStatementsStream.toTable(
                Named.as(outputTopicNames.projectTopStatements()),
                Materialized
                        .<ProjectTopStatementsKey, ProjectTopStatementsValue, KeyValueStore<Bytes, byte[]>>
                                as(outputTopicNames.projectTopStatements() + "-store")
                        .withKeySerde(avroSerdes.ProjectTopStatementsKey())
                        .withValueSerde(avroSerdes.ProjectTopStatementsValue())
        );


        return new ProjectTopStatementsReturnValue(topStatementsTable, topStatementsStream);

    }


}
