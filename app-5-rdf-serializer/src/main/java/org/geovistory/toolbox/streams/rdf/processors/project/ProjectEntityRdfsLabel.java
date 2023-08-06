package org.geovistory.toolbox.streams.rdf.processors.project;

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
public class ProjectEntityRdfsLabel {


    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;


    @Inject
    OutputTopicNames outputTopicNames;

    public ProjectEntityRdfsLabel(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {
         addProcessors(
                 registerInputTopic.projectEntityLabelStream()
        );
    }

    public  ProjectRdfReturnValue addProcessors(
            KStream<ProjectEntityKey, ProjectEntityLabelValue> projectEntityLabelStream
    ) {

        // 2a) group by ProjectEntityKey
        var groupedStream = projectEntityLabelStream.groupByKey(
                Grouped.with(
                        avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityLabelValue()
                ).withName(inner.TOPICS.project_entity_label_grouped)
        );
        // 2b)  aggregate the old and new value to a ProjectRdfList
        List<ArrayList<KeyValue<ProjectRdfKey, ProjectRdfValue>>> projectRdfList = new ArrayList<>();

        KTable<String, Integer> aggregatedTable = groupedStream.aggregate(
                () -> ProjectEntityLabelValue.newBuilder()
                        .build(),
                new Aggregator<String, Integer, Integer>() {
                    @Override
                    public Integer apply(String key, Integer newValue, Integer aggregate) {
                        // Access old value using a state store
                        KeyValueStore<String, Integer> stateStore = (KeyValueStore<String, Integer>) context().getStateStore("old-value-store");
                        Integer oldValue = stateStore.get(key);

                        if (oldValue != null) {
                            // Perform your custom aggregation or calculations here
                            int aggregatedValue = oldValue + newValue;
                            stateStore.put(key, aggregatedValue);
                            return aggregatedValue;
                        } else {
                            stateStore.put(key, newValue);
                            return newValue;
                        }
                    }
                },
                Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("aggregated-store")
                        .withCachingDisabled() // Disable caching to ensure old values are accessed from the state store
        );


        var table = grouped.aggregate(
                () -> ProjectEntityLabelValue.newBuilder()
                        .build(),
                (aggKey, newValue, aggValue) -> {
                    if (aggKey != null) {
                        var v = ProjectRdfValue.newBuilder()
                                .setOperation(Operation.delete)
                                .build();
                        var k = ProjectRdfKey.newBuilder()
                                .setProjectId(newValue.getProjectId())
                                .setTurtle("<" + GEOVISTORY_RESOURCE.getUrl() + newValue.getEntityId() + "> <" + RDFS.getUrl() + "label>\""+ aggValue. +"\" <" + XSD.getUrl() + "string>")
                                .build();
                        result.add(KeyValue.pair(k, v));

                        projectRdfList.add()
                    }
                    aggregate.setParentClasses(value.getDfhParentClasses());
                    return aggregate;
                },
                Materialized.<OntomeClassKey, OntomeClassMetadataValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.ontome_class_metadata_aggregated) /* state store name */
                        .withKeySerde(avroSerdes.OntomeClassKey())
                        .withValueSerde(avroSerdes.OntomeClassMetadataValue())
        );

        var stream = table.toStream(
                Named.as(inner.TOPICS.ontome_class_metadata_aggregated + "-to-stream")
        ).transform(new IdenticalRecordsFilterSupplier<>(
                        "ontome_class_metadata_suppress_duplicates",
                        avroSerdes.OntomeClassKey(),
                        avroSerdes.OntomeClassMetadataValue()),
                Named.as("ontome_class_metadata_suppress_duplicates"));

        /* STREAM PROCESSORS */
        // 2)

        var s = projectEntityLabelStream.flatMap(
            (key, value) -> {
                List<KeyValue<ProjectRdfKey, ProjectRdfValue>> result = new LinkedList<>();

                //value of operation
                var v = ProjectRdfValue.newBuilder()
                        .setOperation(
                                Utils.booleanIsEqualTrue(value.getDeleted$1()) ? Operation.delete : Operation.insert)
                        .build();
                ArrayList<String> turtles = new ArrayList<>();

                //get class ID and label
                var classId = value.getClassId();
                var classLabel = value.getLabel();
                var classLiteralType = (value.getLanguageIso() == null) ? "^^<"+XSD.getUrl()+"string>" : "@"+ value.getLanguageIso();

                turtles.add("<" + ONTOME_CLASS.getUrl() + classId + "> a <" + OWL.getUrl() +"Class> .");
                turtles.add("<" + ONTOME_CLASS.getUrl() + classId + "> <" + RDFS.getUrl() +"label> \""+ classLabel.replaceAll("[\\\\\"]", "\\\\$0") +"\""+ classLiteralType +" .");

                // add the class label triples
                ProjectRdfKey k;
                for (String item : turtles) {
                    k = ProjectRdfKey.newBuilder()
                            .setProjectId(value.getProjectId())
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

    public enum inner {
        TOPICS;
        public final String project_entity_label_grouped = "project_entity_label_grouped";
        public final String project_entity_label_aggregated = "project_entity_label_aggregated";

    }


}
