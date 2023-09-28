package org.geovistory.toolbox.streams.rdf.processors.project;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.IdenticalRecordsFilterSupplier;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.rdf.AvroSerdes;
import org.geovistory.toolbox.streams.rdf.OutputTopicNames;
import org.geovistory.toolbox.streams.rdf.RegisterInputTopic;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.geovistory.toolbox.streams.lib.UrlPrefixes.*;


@ApplicationScoped
public class ProjectOwlProperties {


    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;


    @Inject
    OutputTopicNames outputTopicNames;

    public ProjectOwlProperties(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {
        addProcessors(
                registerInputTopic.projectStatementWithLiteralStream(),
                registerInputTopic.projectStatementWithEntityStream(),
                registerInputTopic.ontomePropertyLabelStream()
        );
    }

    public ProjectRdfReturnValue addProcessors(
            KStream<ProjectStatementKey, ProjectStatementValue> projectStatementWithLiteralStream,
            KStream<ProjectStatementKey, ProjectStatementValue> projectStatementWithEntityStream,
            KStream<OntomePropertyLabelKey, OntomePropertyLabelValue> projectClassLabelStream
    ) {

        /* STREAM PROCESSORS */

        // 2a) project_statement_with_literal group by ProjectOwlPropertyKey
        var groupedStatementWithLiteral = projectStatementWithLiteralStream.groupBy(
                (key, value) ->
                        ProjectOwlPropertyKey.newBuilder()
                                .setProjectId(key.getProjectId())
                                .setPropertyId("p" + value.getStatement().getPropertyId())
                                .build(),
                Grouped.with(
                        avroSerdes.ProjectOwlPropertyKey(), avroSerdes.ProjectStatementValue()
                ));
        // 2b) Aggregate KGroupedStream to stream with ProjectOwlPropertyValue: type='d' (for datatype property)
        var aggregatedProjectDatatypeProperty = groupedStatementWithLiteral.aggregate(
                () -> ProjectOwlPropertyValue.newBuilder().setType("d").build(), /* initializer */
                (aggKey, newValue, aggValue) -> aggValue, /* adder */
                Materialized.<ProjectOwlPropertyKey, ProjectOwlPropertyValue, KeyValueStore<Bytes, byte[]>>as("project_data_property_owl_aggregated-stream-store") /* state store name */
                        .withKeySerde(avroSerdes.ProjectOwlPropertyKey())
                        .withValueSerde(avroSerdes.ProjectOwlPropertyValue())
        );

        // 2c) Suppress duplicates
        var deduplicatedProjectDatatypePropertyStream = aggregatedProjectDatatypeProperty
                .toStream(
                        Named.as("project_data_property_owl_aggregated" + "-to-stream")
                )
                .transform(new IdenticalRecordsFilterSupplier<>("project_data_property_owl_aggregated_suppress_duplicates",
                        avroSerdes.ProjectOwlPropertyKey(), avroSerdes.ProjectOwlPropertyValue()
                ));

        var mappedProjectDatatypeProperty = deduplicatedProjectDatatypePropertyStream.map((key, value) -> {
            var operation = Operation.insert;
            var turtle = "<https://ontome.net/ontology/" + key.getPropertyId() + "> a <http://www.w3.org/2002/07/owl#DatatypeProperty> .";
            var k = ProjectRdfKey.newBuilder().setProjectId(key.getProjectId()).setTurtle(turtle).build();
            var v = ProjectRdfValue.newBuilder().setOperation(operation).build();

            return KeyValue.pair(k, v);

        });

        // 3a)  project_statement_with_entity group by ProjectOwlPropertyKey
        var groupedStatementWithEntity = projectStatementWithEntityStream.groupBy(
                (key, value) ->
                        ProjectOwlPropertyKey.newBuilder()
                                .setProjectId(key.getProjectId())
                                .setPropertyId("p" + value.getStatement().getPropertyId())
                                .build(),
                Grouped.with(
                        avroSerdes.ProjectOwlPropertyKey(), avroSerdes.ProjectStatementValue()
                ));
        // 2b) Aggregate KGroupedStream to stream with ProjectOwlPropertyValue: type='d' (for datatype property)
        var aggregatedProjectObjectProperty = groupedStatementWithEntity.aggregate(
                () -> ProjectOwlPropertyValue.newBuilder().setType("o").build(), /* initializer */
                (aggKey, newValue, aggValue) -> aggValue, /* adder */
                Materialized.<ProjectOwlPropertyKey, ProjectOwlPropertyValue, KeyValueStore<Bytes, byte[]>>as("project_object_property_owl_aggregated-stream-store") /* state store name */
                        .withKeySerde(avroSerdes.ProjectOwlPropertyKey())
                        .withValueSerde(avroSerdes.ProjectOwlPropertyValue())
        );

        // 2c) Suppress duplicates
        var deduplicatedProjectObjectPropertyStream = aggregatedProjectObjectProperty
                .toStream(
                        Named.as("project_object_property_owl_aggregated" + "-to-stream")
                )
                .transform(new IdenticalRecordsFilterSupplier<>("project_object_property_owl_aggregated_suppress_duplicates",
                        avroSerdes.ProjectOwlPropertyKey(), avroSerdes.ProjectOwlPropertyValue()
                ));

        var mappedProjectObjectProperty = deduplicatedProjectObjectPropertyStream.flatMap(
                (key, value) -> {
                    List<KeyValue<ProjectRdfKey, ProjectRdfValue>> result = new LinkedList<>();

                    //value of operation
                    var v = ProjectRdfValue.newBuilder()
                            .setOperation(Operation.insert)
                            .build();
                    ArrayList<String> turtles = new ArrayList<>();

                    //get class ID and label
                    var propertyId = key.getPropertyId());

                    turtles.add("<https://ontome.net/ontology/" + propertyId + "> a <http://www.w3.org/2002/07/owl#ObjectProperty> .");
                    turtles.add("<https://ontome.net/ontology/" + propertyId + "i> a <http://www.w3.org/2002/07/owl#ObjectProperty> .");
                    turtles.add("<https://ontome.net/ontology/" + propertyId + "i> <http://www.w3.org/2002/07/owl#inverseOf> <https://ontome.net/ontology/"+propertyId+"> .");

                    // add the class label triples
                    ProjectRdfKey k;
                    for (String item : turtles) {
                        k = ProjectRdfKey.newBuilder()
                                .setProjectId(key.getProjectId())
                                .setTurtle(item)
                                .build();
                        result.add(KeyValue.pair(k, v));
                    }
                    return result;
                }
        );

        /* SINK PROCESSORS */

        s.to(outputTopicNames.projectRdf(),
                Produced.with(avroSerdes.ProjectRdfKey(), avroSerdes.ProjectRdfValue())
                        .withName(outputTopicNames.projectRdf() + "-class-label-producer")
        );


        return new ProjectRdfReturnValue(s);

    }


}