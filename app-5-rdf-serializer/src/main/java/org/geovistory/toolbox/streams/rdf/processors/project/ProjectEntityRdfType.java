package org.geovistory.toolbox.streams.rdf.processors.project;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.rdf.AvroSerdes;
import org.geovistory.toolbox.streams.rdf.OutputTopicNames;
import org.geovistory.toolbox.streams.rdf.RegisterInputTopic;

import java.util.LinkedList;
import java.util.List;

import static org.geovistory.toolbox.streams.lib.UrlPrefixes.*;


@ApplicationScoped
public class ProjectEntityRdfType {


    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;


    @Inject
    OutputTopicNames outputTopicNames;

    public ProjectEntityRdfType(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {
        addProcessors(
                registerInputTopic.projectEntityStream()
        );
    }

    public ProjectRdfReturnValue addProcessors(
            KStream<ProjectEntityKey, ProjectEntityValue> projectEntityLabelStream
    ) {

        // 2a) Group by ProjectEntityKey
        var groupedStream = projectEntityLabelStream.groupByKey(
                Grouped.with(
                        avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityValue()
                ).withName(inner.TOPICS.project_entity_grouped)
        );

        //  2b) Aggregate the old and new value to a ProjectRdfList
        KTable<ProjectEntityKey, ProjectRdfList> aggregatedStream = groupedStream.aggregate(
                () -> ProjectRdfList.newBuilder().build(),
                (aggKey, newValue, aggValue) -> {
                    // we create a new ProjectRdfRecord for the new class
                    var newRdfValueOp = Utils.booleanIsEqualTrue(newValue.getDeleted$1()) ? Operation.delete : Operation.insert;
                    var newRdfValue = ProjectRdfValue.newBuilder().setOperation(newRdfValueOp).build();
                    var newRdfKey = ProjectRdfKey.newBuilder().setProjectId(aggKey.getProjectId())
                            .setTurtle("<" + GEOVISTORY_RESOURCE.getUrl() + aggKey.getEntityId() + "> <" + RDF.getUrl() + "type> <" + ONTOME_CLASS.getUrl() + newValue.getClassId() + "> .")
                            .build();
                    var newRdfRecord = ProjectRdfRecord.newBuilder().setKey(newRdfKey).setValue(newRdfValue).build();


                    // if: size 2 -> we remove the first item from list, as it is not relevant anymore.
                    if (aggValue.getList().size() == 2) {
                        aggValue.getList().remove(0);
                    }

                    // case: size 1
                    if (aggValue.getList().size() == 1) {
                        var oldVal = aggValue.getList().get(0).getValue();
                        var oldKey = aggValue.getList().get(0).getKey();
                        // if operation was delete, remove it
                        if (oldVal.getOperation() == Operation.delete) {
                            aggValue.getList().remove(0);
                        }
                        // if the newVal is identical to the oldVal
                        else if (oldVal.equals(newRdfValue) && oldKey.equals(newRdfKey)) {
                            // we return the list as is
                            return aggValue;
                        }
                        // else we modify the operation from 'insert' to 'delete'
                        else {
                            oldVal.setOperation(Operation.delete);
                        }
                    }
                    // case: size 0 -> do nothing

                    // add the new rdf record to the agg value
                    aggValue.getList().add(newRdfRecord);

                    return aggValue;
                },
                Materialized.<ProjectEntityKey, ProjectRdfList, KeyValueStore<Bytes, byte[]>>as("aggregated-project-entity-rdf-type-store") /* state store name */
                        .withKeySerde(
                                avroSerdes.ProjectEntityKey()
                        )
                        .withValueSerde(
                                avroSerdes.ProjectRdfList()
                        )
        );

        // 2c) FlapMap the records
        var projectRdfStream = aggregatedStream.toStream().flatMap(
                (key, value) -> {
                    List<KeyValue<ProjectRdfKey, ProjectRdfValue>> result = new LinkedList<>();
                    value.getList().forEach(projectRdfRecord -> result.add(KeyValue.pair(projectRdfRecord.getKey(), projectRdfRecord.getValue())));
                    return result;
                }
        );


        /* SINK PROCESSORS */
        // 3a) Sink to project_rdf
        projectRdfStream.to(outputTopicNames.projectRdf(),
                Produced.with(avroSerdes.ProjectRdfKey(), avroSerdes.ProjectRdfValue())
                        .withName(outputTopicNames.projectRdf() + "-project-entity-producer")
        );


        return new ProjectRdfReturnValue(projectRdfStream);

    }

    public enum inner {
        TOPICS;
        public final String project_entity_grouped = "project_entity_grouped";
    }


}
