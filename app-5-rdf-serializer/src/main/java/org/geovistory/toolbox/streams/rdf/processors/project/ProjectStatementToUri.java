package org.geovistory.toolbox.streams.rdf.processors.project;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.rdf.AvroSerdes;
import org.geovistory.toolbox.streams.rdf.OutputTopicNames;
import org.geovistory.toolbox.streams.rdf.RegisterInputTopic;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;

import static org.geovistory.toolbox.streams.lib.UrlPrefixes.GEOVISTORY_RESOURCE;
import static org.geovistory.toolbox.streams.lib.UrlPrefixes.ONTOME_PROPERTY;


@ApplicationScoped
public class ProjectStatementToUri {


    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;


    @Inject
    OutputTopicNames outputTopicNames;

    public ProjectStatementToUri(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {
         addProcessors(
                registerInputTopic.projectStatementWithEntityStream()
        );
    }

    public  ProjectRdfReturnValue addProcessors(
            KStream<ProjectStatementKey, ProjectStatementValue> projectStatementWithEntityStream
    ) {

        /* STREAM PROCESSORS */
        // 2)

        var s = projectStatementWithEntityStream.flatMap(
                (key, value) -> {
                    List<KeyValue<ProjectRdfKey, ProjectRdfValue>> result = new LinkedList<>();

                    //value of operation
                    var v = ProjectRdfValue.newBuilder()
                            .setOperation(
                                    Utils.booleanIsEqualTrue(value.getDeleted$1()) ? Operation.delete : Operation.insert)
                            .build();

                    //get subject, object and property ids
                    var subjectId = value.getStatement().getSubjectId();
                    var objectId = value.getStatement().getObjectId();
                    var propertyId = value.getStatement().getPropertyId();

                    // add the normal triple
                    var k = ProjectRdfKey.newBuilder()
                            .setProjectId(value.getProjectId())
                            .setTurtle("<" + GEOVISTORY_RESOURCE.getUrl() + subjectId + "> <" + ONTOME_PROPERTY.getUrl() + propertyId + "> <" + GEOVISTORY_RESOURCE.getUrl() + objectId + ">")
                            .build();
                    result.add(KeyValue.pair(k, v));

                    var ki = ProjectRdfKey.newBuilder()
                            .setProjectId(value.getProjectId())
                            .setTurtle("<" + GEOVISTORY_RESOURCE.getUrl() + objectId + "> <" + ONTOME_PROPERTY.getUrl() + propertyId + "i> <" + GEOVISTORY_RESOURCE.getUrl() + subjectId + ">")
                            .build();
                    result.add(KeyValue.pair(ki, v));

                    return result;
                }
        );
        /* SINK PROCESSORS */

        s.to(outputTopicNames.projectRdf(),
                Produced.with(avroSerdes.ProjectRdfKey(), avroSerdes.ProjectRdfValue())
                        .withName(outputTopicNames.projectRdf() + "-entity-producer")
        );


        return new ProjectRdfReturnValue(s);

    }


}
