package org.geovistory.toolbox.streams.topologies;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.geovistory.toolbox.streams.app.RegisterOutputTopic;
import org.geovistory.toolbox.streams.avro.ProjectTopStatementsKey;
import org.geovistory.toolbox.streams.avro.ProjectTopStatementsValue;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;


public class ProjectTopStatements {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerOutputTopic = new RegisterOutputTopic(builder);

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
        var topStatementsStream = outgoingTopStatementsStream.merge(incomingTopStatementsStream);

        /* SINK PROCESSORS */

        topStatementsStream.to(output.TOPICS.project_top_statements,
                Produced.with(avroSerdes.ProjectTopStatementsKey(), avroSerdes.ProjectTopStatementsValue()));

        var topStatementsTable = topStatementsStream.toTable(
                Named.as(output.TOPICS.project_top_statements),
                Materialized.with(avroSerdes.ProjectTopStatementsKey(), avroSerdes.ProjectTopStatementsValue())
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