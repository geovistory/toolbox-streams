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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.geovistory.toolbox.streams.lib.UrlPrefixes.GEOVISTORY_RESOURCE;
import static org.geovistory.toolbox.streams.lib.UrlPrefixes.ONTOME_PROPERTY;


@ApplicationScoped
public class ProjectCustomRdfsLabels {


    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;


    @Inject
    OutputTopicNames outputTopicNames;

    public ProjectCustomRdfsLabels(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {
         addProcessors(
                registerInputTopic.projectStream()
        );
    }

    public  ProjectRdfReturnValue addProcessors(
            KStream<ProjectRdfKey, ProjectRdfValue> projectStream
    ) {

        /* STREAM PROCESSORS */
        // 2)

        var s = projectStream.flatMap(
                (key, value) -> {
                    List<KeyValue<ProjectRdfKey, ProjectRdfValue>> result = new LinkedList<>();
                    ArrayList<String> turtles = new ArrayList<>();

                    //value of operation
                    var v = ProjectRdfValue.newBuilder()
                            .setOperation(Operation.insert)
                            .build();

                    //turtles.add("<" + GEOVISTORY_RESOURCE.getUrl() + subjectId + "ts> <" + ONTOME_PROPERTY.getUrl() + propertyId + "> <" + GEOVISTORY_RESOURCE.getUrl() + objectId + ">");


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
                        .withName(outputTopicNames.projectRdf() + "-custom-rdfs-labels-producer")
        );


        return new ProjectRdfReturnValue(s);

    }


}
