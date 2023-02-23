package org.geovistory.toolbox.streams.entity.processors.project;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectEntityTopStatementsValue;
import org.geovistory.toolbox.streams.avro.TimeSpanValue;
import org.geovistory.toolbox.streams.entity.RegisterInnerTopic;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.LinkedList;
import java.util.List;

import static org.geovistory.toolbox.streams.entity.lib.TimeSpanFactory.createTimeSpan;


public class ProjectEntityTimeSpan {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {

        var innerTopic = new RegisterInnerTopic(builder);

        var projectEntityTopStatementsValueStream = innerTopic.projectEntityTopStatementsStream();

        return addProcessors(builder, projectEntityTopStatementsValueStream).builder().build();
    }

    public static ProjectEntityTimeSpanReturnValue addProcessors(
            StreamsBuilder builder,
            KStream<ProjectEntityKey, ProjectEntityTopStatementsValue> projectEntityTopStatementsValueStream
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
                Named.as("kstream-flat-map-project-entity-top-statements-to-time-span")
        );



        /* SINK PROCESSORS */
        stream.to(output.TOPICS.project_entity_time_span,
                Produced.with(avroSerdes.ProjectEntityKey(), avroSerdes.TimeSpanValue())
                        .withName(output.TOPICS.project_entity_time_span + "-producer")
        );

        return new ProjectEntityTimeSpanReturnValue(builder, stream);

    }



    public enum input {
        TOPICS;
        public final String project_entity_top_statements = ProjectEntityTopStatements.output.TOPICS.project_entity_top_statements;


    }


    public enum output {
        TOPICS;
        public final String project_entity_time_span = Utils.tsPrefixed("project_entity_time_span");

    }

}
