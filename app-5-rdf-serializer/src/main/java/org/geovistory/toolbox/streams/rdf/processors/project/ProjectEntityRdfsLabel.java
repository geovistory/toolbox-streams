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

    public ProjectRdfReturnValue addProcessors(
            KStream<ProjectEntityKey, ProjectEntityLabelValue> projectEntityLabelStream
    ) {

        // 2a) group by ProjectEntityKey
        var groupedStream = projectEntityLabelStream.groupByKey(
                Grouped.with(
                        avroSerdes.ProjectEntityKey(), avroSerdes.ProjectEntityLabelValue()
                ).withName(inner.TOPICS.project_entity_label_grouped)
        );

        // Aggregating a KGroupedStream (note how the value type changes from String to Long)
        KTable<ProjectEntityKey, ProjectRdfList> aggregatedStream = groupedStream.aggregate(
                () -> ProjectRdfList.newBuilder().build(),
                (aggKey, newValue, aggValue) -> {
                    // we create a new ProjectRdfRecord for the new label
                    var newRdfValueOp = newValue.getDeleted$1() ? Operation.delete : Operation.insert;
                    var newRdfValue = ProjectRdfValue.newBuilder().setOperation(newRdfValueOp).build();
                    var newRdfKey = ProjectRdfKey.newBuilder().setProjectId(aggKey.getProjectId())
                            .setTurtle("<" + GEOVISTORY_RESOURCE.getUrl() + aggKey.getEntityId() + "> <" + RDFS.getUrl() + "label> \"" + newValue.getLabel() + "\"@^^<" + XSD.getUrl() + "string> .")
                            .build();
                    var newRdfRecord = ProjectRdfRecord.newBuilder().setKey(newRdfKey).setValue(newRdfValue).build();


                    // if: size 2 -> we remove the first item from list, as it is not relevant anymore.
                    if (aggValue.getList().size() == 2) {
                        aggValue.getList().remove(0);
                    }

                    // case: size 1
                    if (aggValue.getList().size() == 1) {
                        var oldVal = aggValue.getList().get(0).getValue();
                        // if operation was delete, remove it
                        if (oldVal.getOperation() == Operation.delete) {
                            aggValue.getList().remove(0);
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
                Materialized.<ProjectEntityKey, ProjectRdfList, KeyValueStore<Bytes, byte[]>>as("aggregated-project-entity-rdfs-label-store") /* state store name */
                        .withKeySerde(
                                avroSerdes.ProjectEntityKey()
                        )
                        .withValueSerde(
                                avroSerdes.ProjectRdfList()
                        )
        );

        var projectRdfStream = aggregatedStream.toStream().flatMap(
                (key, value) -> {
                    List<KeyValue<ProjectRdfKey, ProjectRdfValue>> result = new LinkedList<>();
                    value.getList().forEach(projectRdfRecord -> {
                        result.add(KeyValue.pair(projectRdfRecord.getKey(), projectRdfRecord.getValue()));
                    });
                    return result;
                }
        );


        /* SINK PROCESSORS */

        projectRdfStream.to(outputTopicNames.projectRdf(),
                Produced.with(avroSerdes.ProjectRdfKey(), avroSerdes.ProjectRdfValue())
                        .withName(outputTopicNames.projectRdf() + "-class-label-producer")
        );


        return new ProjectRdfReturnValue(projectRdfStream);

    }

    public enum inner {
        TOPICS;
        public final String project_entity_label_grouped = "project_entity_label_grouped";
        public final String project_entity_label_aggregated = "project_entity_label_aggregated";

    }


}
