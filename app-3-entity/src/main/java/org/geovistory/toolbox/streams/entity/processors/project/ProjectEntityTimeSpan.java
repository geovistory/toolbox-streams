package org.geovistory.toolbox.streams.entity.processors.project;

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


public class ProjectEntityTimeSpan {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {

        var innerTopic = new RegisterInputTopic(builder);

        var projectEntityTopStatementsValueStream = innerTopic.projectTopOutgoingStatementsStream();

        return addProcessors(builder, projectEntityTopStatementsValueStream).builder().build();
    }

    public static ProjectEntityTimeSpanReturnValue addProcessors(
            StreamsBuilder builder,
            KStream<ProjectTopStatementsKey, ProjectTopStatementsValue> projectOutgoingTopStatements
    ) {

        var avroSerdes = new ConfluentAvroSerdes();


        /* STREAM PROCESSORS */
        // 2)

        var stream = projectOutgoingTopStatements.flatMapValues(
                (key, value) -> {
                    var res = new LinkedList<TopTimePrimitives>();

                    // suppress if not time property
                    if (!TimeSpanFactory.isTimeProperty(key.getPropertyId())) return res;

                    var topTimePrimitives = TopTimePrimitives.newBuilder()
                            .setTimePrimitives(new ArrayList<>())
                            .setPropertyId(key.getPropertyId());

                    // convert statements to time primitives
                    var array = topTimePrimitives.getTimePrimitives();
                    for (var s : value.getStatements()) {
                        var tp = extractTimePrimitive(s);
                        if (tp != null) array.add(tp);
                    }

                    res.add(topTimePrimitives.build());

                    return res;
                },
                Named.as("kstream-flat-map-project-top-time-primitives")
        );

        var grouped = stream.groupBy(
                (key, value) -> ProjectEntityKey.newBuilder()
                        .setProjectId(key.getProjectId())
                        .setEntityId(key.getEntityId())
                        .build(),
                Grouped.with(
                        inner.project_top_time_primitives_grouped.toString(),
                        avroSerdes.ProjectEntityKey(),
                        avroSerdes.TopTimePrimitives()
                )
        );

        var aggregated = grouped.aggregate(
                () -> TopTimePrimitivesMap.newBuilder().build(),
                (key, value, aggregate) -> {
                    aggregate.getMap().put("" + value.getPropertyId(), value);
                    return aggregate;
                },
                Materialized.<ProjectEntityKey, TopTimePrimitivesMap, KeyValueStore<Bytes, byte[]>>as(inner.project_top_time_primitives_aggregated.toString())
                        .withKeySerde(avroSerdes.ProjectEntityKey())
                        .withValueSerde(avroSerdes.TopTimePrimitivesMap())
        );

        var timeSpanStream = aggregated
                .toStream(
                        Named.as("to-stream-" + inner.project_top_time_primitives_aggregated)
                )
                .mapValues((readOnlyKey, value) -> TimeSpanFactory.createTimeSpan(value));


        /* SINK PROCESSORS */
        timeSpanStream.to(output.TOPICS.project_entity_time_span,
                Produced.with(avroSerdes.ProjectEntityKey(), avroSerdes.TimeSpanValue())
                        .withName(output.TOPICS.project_entity_time_span + "-producer")
        );

        return new ProjectEntityTimeSpanReturnValue(builder, timeSpanStream);

    }

    public static TimePrimitive extractTimePrimitive(ProjectStatementValue value) {
        var a = value.getStatement();
        if (a == null) return null;

        var b = a.getObject();
        if (b == null) return null;

        return b.getTimePrimitive();
    }





    public enum inner {
        project_top_time_primitives_grouped,
        project_top_time_primitives_aggregated

    }


    public enum output {
        TOPICS;
        public final String project_entity_time_span = Utils.tsPrefixed("project_entity_time_span");

    }

}
