package org.geovistory.toolbox.streams.entity.label.processsors.community;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.geovistory.toolbox.streams.avro.ProjectStatementKey;
import org.geovistory.toolbox.streams.avro.ProjectStatementValue;
import org.geovistory.toolbox.streams.entity.label.RegisterInnerTopic;
import org.geovistory.toolbox.streams.entity.label.processsors.project.ProjectStatementWithLiteral;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;


public class CommunityToolboxStatementWithLiteral {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var innerTopic = new RegisterInnerTopic(builder);

        return addProcessors(
                builder,
                innerTopic.projectStatementWithLiteralStream()
        ).builder().build();
    }

    public static CommunityToolboxStatementReturnValue addProcessors(
            StreamsBuilder builder,
            KStream<ProjectStatementKey, ProjectStatementValue> projectStatementWithLiteralStream) {

        var avroSerdes = new ConfluentAvroSerdes();

        var result = projectStatementWithLiteralStream
                .transform(new CommunityToolboxStatementCounterSupplier("community_toolbox_statement_with_literal_counter"));
        result.to(output.TOPICS.community_toolbox_statement_with_literal,
                Produced.with(avroSerdes.CommunityStatementKey(), avroSerdes.CommunityStatementValue())
                        .withName(output.TOPICS.community_toolbox_statement_with_literal + "-producer")
        );

        return new CommunityToolboxStatementReturnValue(builder, result);

    }


    public enum input {
        TOPICS;
        public final String project_statement_with_literal = ProjectStatementWithLiteral.output.TOPICS.project_statement_with_literal;
    }


    public enum inner {
        TOPICS
    }

    public enum output {
        TOPICS;
        public final String community_toolbox_statement_with_literal = Utils.tsPrefixed("community_toolbox_statement_with_literal");
    }
}
