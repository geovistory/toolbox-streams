package org.geovistory.toolbox.streams.project.entity.label.processors;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.ProjectTopStatementsKey;
import org.geovistory.toolbox.streams.avro.ProjectTopStatementsValue;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.project.entity.label.RegisterInnerTopic;


public class ProjectTopStatements {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerOutputTopic = new RegisterInnerTopic(builder);

        return addProcessors(
                builder,
                registerOutputTopic.projectTopOutgoingStatementsStream(),
                registerOutputTopic.projectTopIncomingStatementsStream()
        ).builder().build();
    }

    public static ProjectTopStatementsReturnValue addProcessors(
            StreamsBuilder builder,
            KStream<ProjectTopStatementsKey, ProjectTopStatementsValue> outgoingTopStatementsStream,
            KStream<ProjectTopStatementsKey, ProjectTopStatementsValue> incomingTopStatementsStream
    ) {

        var avroSerdes = new ConfluentAvroSerdes();


        /* STREAM PROCESSORS */
        // 2)
        var topStatementsStream = outgoingTopStatementsStream.merge(
                incomingTopStatementsStream,
                Named.as("kstream-merge-project-top-out-s-and-project-top-in-s")
        );

        /* SINK PROCESSORS */

        topStatementsStream.to(output.TOPICS.project_top_statements,
                Produced.with(avroSerdes.ProjectTopStatementsKey(), avroSerdes.ProjectTopStatementsValue())
                        .withName(output.TOPICS.project_top_statements + "-producer")
        );

        var topStatementsTable = topStatementsStream.toTable(
                Named.as(output.TOPICS.project_top_statements),
                Materialized
                        .<ProjectTopStatementsKey, ProjectTopStatementsValue, KeyValueStore<Bytes, byte[]>>
                                as(output.TOPICS.project_top_statements + "-store")
                        .withKeySerde(avroSerdes.ProjectTopStatementsKey())
                        .withValueSerde(avroSerdes.ProjectTopStatementsValue())
        );


        return new ProjectTopStatementsReturnValue(builder, topStatementsTable, topStatementsStream);

    }


    public enum input {
        TOPICS;
        public final String project_top_outgoing_statements = ProjectTopOutgoingStatements.output.TOPICS.project_top_outgoing_statements;
        public final String project_top_incoming_statements = ProjectTopIncomingStatements.output.TOPICS.project_top_incoming_statements;
    }


    public enum output {
        TOPICS;
        public final String project_top_statements = Utils.tsPrefixed("project_top_statements");
    }

}
