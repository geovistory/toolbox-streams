package org.geovistory.toolbox.streams.entity.processors.project;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.AvroSerdes;
import org.geovistory.toolbox.streams.entity.OutputTopicNames;
import org.geovistory.toolbox.streams.entity.RegisterInputTopic;
import org.geovistory.toolbox.streams.entity.lib.TimeSpanFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedList;


@ApplicationScoped
public class ProjectEntityTimeSpan {

    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;

    @Inject
    OutputTopicNames outputTopicNames;

    public ProjectEntityTimeSpan(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {
        addProcessors(
                registerInputTopic.projectTopOutgoingStatementsStream()
        );
    }

    public ProjectEntityTimeSpanReturnValue addProcessors(
            KStream<ProjectTopStatementsKey, ProjectTopStatementsValue> projectOutgoingTopStatements
    ) {

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
                    for (var s : value.getEdges()) {
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
        timeSpanStream.to(outputTopicNames.projectEntityTimeSpan(),
                Produced.with(avroSerdes.ProjectEntityKey(), avroSerdes.TimeSpanValue())
                        .withName(outputTopicNames.projectEntityTimeSpan() + "-producer")
        );

        return new ProjectEntityTimeSpanReturnValue(timeSpanStream);

    }

    public static TimePrimitive extractTimePrimitive(ProjectEdgeValue value) {
        var n = value.getTargetNode();

        if (n == null) return null;

        return n.getTimePrimitive();
    }


    public enum inner {
        project_top_time_primitives_grouped,
        project_top_time_primitives_aggregated

    }


}
