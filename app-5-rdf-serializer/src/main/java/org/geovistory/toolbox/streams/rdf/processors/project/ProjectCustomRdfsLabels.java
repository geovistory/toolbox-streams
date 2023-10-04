package org.geovistory.toolbox.streams.rdf.processors.project;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.rdf.AvroSerdes;
import org.geovistory.toolbox.streams.rdf.OutputTopicNames;
import org.geovistory.toolbox.streams.rdf.RegisterInputTopic;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.geovistory.toolbox.streams.lib.UrlPrefixes.*;


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

    public ProjectRdfReturnValue addProcessors(
            KStream<dev.projects.project.Key, dev.projects.project.Value> projectStream
    ) {

        /* STREAM PROCESSORS */
        // 2a) FlatMap the records to ProjectRdfKey and ProjectRdfValue to the triples in the "turtles" list

        var s = projectStream.flatMap(
                (key, value) -> {
                    List<KeyValue<ProjectRdfKey, ProjectRdfValue>> result = new LinkedList<>();
                    ArrayList<String> turtles = new ArrayList<>();

                    //value of operation
                    var v = ProjectRdfValue.newBuilder()
                            .setOperation(Operation.insert)
                            .build();

                    turtles.add("<" + RDF.getUrl() + "type> <" + RDFS.getUrl() + "label> \"has type\"@en .");
                    turtles.add("<" + RDFS.getUrl() + "label> <" + RDFS.getUrl() + "label> \"has label\"@en .");
                    turtles.add("<" + OWL.getUrl() + "sameAs> <" + RDFS.getUrl() + "label> \"same as\"@en .");

                    turtles.add("<" + ONTOME_CLASS.getUrl() + "50> <" + RDFS.getUrl() + "label> \"Time-Span\"@en .");
                    turtles.add("<" + ONTOME_CLASS.getUrl() + "50> a <" + OWL.getUrl() + "Class> .");
                    turtles.add("<" + ONTOME_PROPERTY.getUrl() + "4> <" + RDFS.getUrl() + "label> \"has time-span\"@en .");
                    turtles.add("<" + ONTOME_PROPERTY.getUrl() + "4i> <" + RDFS.getUrl() + "label> \"is time-span of\"@en .");

                    turtles.add("<" + TIME.getUrl() + "DateTimeDescription> <" + RDFS.getUrl() + "label> \"Date-Time description\"@en .");
                    turtles.add("<" + TIME.getUrl() + "hasTRS> <" + RDFS.getUrl() + "label> \"Temporal reference system used\"@en .");
                    turtles.add("<" + TIME.getUrl() + "unitType> <" + RDFS.getUrl() + "label> \"temporal unit type\"@en .");
                    turtles.add("<" + TIME.getUrl() + "year> <" + RDFS.getUrl() + "label> \"Year\"@en .");
                    turtles.add("<" + TIME.getUrl() + "month> <" + RDFS.getUrl() + "label> \"Month\"@en .");
                    turtles.add("<" + TIME.getUrl() + "day> <" + RDFS.getUrl() + "label> \"Day\"@en .");
                    turtles.add("<" + TIME.getUrl() + "unitYear> <" + RDFS.getUrl() + "label> \"Year (unit of temporal duration)\"@en .");
                    turtles.add("<" + TIME.getUrl() + "unitMonth> <" + RDFS.getUrl() + "label> \"Month (unit of temporal duration)\"@en .");
                    turtles.add("<" + TIME.getUrl() + "unitDay> <" + RDFS.getUrl() + "label> \"Day (unit of temporal duration)\"@en .");

                    turtles.add("<" + GREG.getUrl() + "> <" + RDFS.getUrl() + "label> \"Gregorian Calendar\"@en .");
                    turtles.add("<" + JUL.getUrl() + "> <" + RDFS.getUrl() + "label> \"Julian Calendar\"@en .");



                    ProjectRdfKey k;
                    for (String item : turtles) {
                        k = ProjectRdfKey.newBuilder()
                                .setProjectId(key.getPkEntity())
                                .setTurtle(item)
                                .build();
                        result.add(KeyValue.pair(k, v));
                    }

                    return result;
                }
        );
        /* SINK PROCESSORS */
        //3a) To: sink it to project_rdf
        s.to(outputTopicNames.projectRdf(),
                Produced.with(avroSerdes.ProjectRdfKey(), avroSerdes.ProjectRdfValue())
                        .withName(outputTopicNames.projectRdf() + "-custom-rdfs-labels-producer")
        );


        return new ProjectRdfReturnValue(s);

    }


}
