package org.geovistory.toolbox.streams.rdf.processors.project;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.IdenticalRecordsFilterSupplier;
import org.geovistory.toolbox.streams.lib.StringSanitizer;
import org.geovistory.toolbox.streams.rdf.AvroSerdes;
import org.geovistory.toolbox.streams.rdf.OutputTopicNames;
import org.geovistory.toolbox.streams.rdf.RegisterInputTopic;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

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
            KStream<OntomePropertyLabelKey, OntomePropertyLabelValue> ontomePropertyLabelStream
    ) {

        /* STREAM PROCESSORS */

        // 2a) project_statement_with_literal group by ProjectOwlPropertyKey
        var groupedStatementWithLiteral = projectStatementWithLiteralStream.groupBy(
                (key, value) ->
                        ProjectOwlPropertyKey.newBuilder()
                                .setProjectId(key.getProjectId())
                                .setPropertyId(value.getStatement().getPropertyId())
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
            var turtle = "<https://ontome.net/ontology/p" + key.getPropertyId() + "> a <http://www.w3.org/2002/07/owl#DatatypeProperty> .";
            var k = ProjectRdfKey.newBuilder().setProjectId(key.getProjectId()).setTurtle(turtle).build();
            var v = ProjectRdfValue.newBuilder().setOperation(operation).build();

            return KeyValue.pair(k, v);

        });

        // 3a)  project_statement_with_entity group by ProjectOwlPropertyKey
        var groupedStatementWithEntity = projectStatementWithEntityStream.groupBy(
                (key, value) ->
                        ProjectOwlPropertyKey.newBuilder()
                                .setProjectId(key.getProjectId())
                                .setPropertyId(value.getStatement().getPropertyId())
                                .build(),
                Grouped.with(
                        avroSerdes.ProjectOwlPropertyKey(), avroSerdes.ProjectStatementValue()
                ));
        // 3b) Aggregate KGroupedStream to stream with ProjectOwlPropertyValue: type='o' (for object property)
        var aggregatedProjectObjectProperty = groupedStatementWithEntity.aggregate(
                () -> ProjectOwlPropertyValue.newBuilder().setType("o").build(), /* initializer */
                (aggKey, newValue, aggValue) -> aggValue, /* adder */
                Materialized.<ProjectOwlPropertyKey, ProjectOwlPropertyValue, KeyValueStore<Bytes, byte[]>>as("project_object_property_owl_aggregated-stream-store") /* state store name */
                        .withKeySerde(avroSerdes.ProjectOwlPropertyKey())
                        .withValueSerde(avroSerdes.ProjectOwlPropertyValue())
        );

        // 3c) Suppress duplicates
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

                    var v = ProjectRdfValue.newBuilder()
                            .setOperation(Operation.insert)
                            .build();

                    ArrayList<String> turtles = new ArrayList<>();

                    var propertyId = key.getPropertyId();

                    turtles.add("<https://ontome.net/ontology/p" + propertyId + "> a <http://www.w3.org/2002/07/owl#ObjectProperty> .");
                    turtles.add("<https://ontome.net/ontology/p" + propertyId + "i> a <http://www.w3.org/2002/07/owl#ObjectProperty> .");
                    turtles.add("<https://ontome.net/ontology/p" + propertyId + "i> <http://www.w3.org/2002/07/owl#inverseOf> <https://ontome.net/ontology/p" + propertyId + "> .");

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

        // 4a) and 4b) Merge the streams of 2c and 3c into a KTable
        var mergedProjectPropertyTable = deduplicatedProjectDatatypePropertyStream.merge(deduplicatedProjectObjectPropertyStream)
                .mapValues((readOnlyKey, value) -> readOnlyKey)
                .toTable(
                        Named.as("mergedProjectPropertyTable-to-table"),
                        Materialized
                                .<ProjectOwlPropertyKey, ProjectOwlPropertyKey, KeyValueStore<Bytes, byte[]>>
                                        as("mergedProjectPropertyTable-store")
                                .withKeySerde(avroSerdes.ProjectOwlPropertyKey())
                                .withValueSerde(avroSerdes.ProjectOwlPropertyKeyAsValue())
                );


        // 4c) Join the english ontome property using a foreign key join, creating ProjectOwlPropertyLabelValue
        // Define the ValueJoiner with appropriate types
        ValueJoiner<ProjectOwlPropertyKey, OntomePropertyLabelValue, ProjectOwlPropertyLabelValue> valueJoiner = (value1, value2) -> {
            var res = ProjectOwlPropertyLabelValue.newBuilder();
            if (value2 == null) return res.build();
            // Combine the values as needed to create a ResultValue
            return res.setLabel(value2.getLabel()).setInverseLabel(value2.getInverseLabel()).build();
        };

        Materialized<ProjectOwlPropertyKey, ProjectOwlPropertyLabelValue, KeyValueStore<Bytes, byte[]>> materialized =
                Materialized.<ProjectOwlPropertyKey, ProjectOwlPropertyLabelValue, KeyValueStore<Bytes, byte[]>>as("project_entity_fulltext_join_prop_label")
                        .withKeySerde(avroSerdes.ProjectOwlPropertyKey())
                        .withValueSerde(avroSerdes.ProjectOwlPropertyLabelValue());
        var ontomePropertyLabelTable = ontomePropertyLabelStream.toTable();


        var joinedProjectPropertyWithOntomeLabel = mergedProjectPropertyTable.leftJoin(
                ontomePropertyLabelTable,
                projectOwlPropertyKey -> OntomePropertyLabelKey.newBuilder()
                        .setPropertyId(projectOwlPropertyKey.getPropertyId())
                        .setLanguageId(18889)
                        .build(),
                valueJoiner,
                TableJoined.as("project_owl_prop_join_ontome_prop_label" + "-fk-left-join"),
                materialized
        );

        // 4d) FlatMap to ProjectRdfKey and ProjectRdfValue, with insert operations
        var mappedProjectPropertyOntomeLabel = joinedProjectPropertyWithOntomeLabel.toStream().flatMap(
                (key, value) -> {
                    List<KeyValue<ProjectRdfKey, ProjectRdfValue>> result = new LinkedList<>();

                    //value of operation
                    var v = ProjectRdfValue.newBuilder().setOperation(Operation.insert).build();
                    ArrayList<String> turtles = new ArrayList<>();

                    //get class ID and label
                    var propertyId = key.getPropertyId();

                    if (value.getLabel() != null) {
                        turtles.add("<https://ontome.net/ontology/p" + propertyId + "> <http://www.w3.org/2000/01/rdf-schema#label> \"" + StringSanitizer.escapeJava(value.getLabel()) + "\"@en .");

                    }

                    if (value.getInverseLabel() != null) {
                        turtles.add("<https://ontome.net/ontology/p" + propertyId + "i> <http://www.w3.org/2000/01/rdf-schema#label> \"" + StringSanitizer.escapeJava(value.getInverseLabel()) + "\"@en .");
                    }

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

        var s = mappedProjectObjectProperty.merge(mappedProjectDatatypeProperty).merge(mappedProjectPropertyOntomeLabel);

        s.to(outputTopicNames.projectRdf(),
                Produced.with(avroSerdes.ProjectRdfKey(), avroSerdes.ProjectRdfValue())
                        .withName(outputTopicNames.projectRdf() + "-project-owl-property-producer")
        );


        return new ProjectRdfReturnValue(s);

    }
}