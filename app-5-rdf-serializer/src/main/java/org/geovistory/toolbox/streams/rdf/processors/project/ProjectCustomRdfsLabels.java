package org.geovistory.toolbox.streams.rdf.processors.project;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.geovistory.toolbox.streams.avro.*;
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
                    /**
                     * <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2000/01/rdf-schema#label> "has type"@en .
                     * <http://www.w3.org/2000/01/rdf-schema#label> <http://www.w3.org/2000/01/rdf-schema#label> "has label"@en .
                     * <http://www.w3.org/2002/07/owl#sameAs> <http://www.w3.org/2000/01/rdf-schema#label> "same as"@en .
                     *
                     * <https://ontome.net/ontology/c50> <http://www.w3.org/2000/01/rdf-schema#label> "Time-Span"@en .
                     * <https://ontome.net/ontology/c50> a <http://www.w3.org/2002/07/owl#Class> .
                     * <https://ontome.net/ontology/p4> <http://www.w3.org/2000/01/rdf-schema#label> "has time-span"@en .
                     * <https://ontome.net/ontology/p4i> <http://www.w3.org/2000/01/rdf-schema#label> "is time-span of"@en .
                     *
                     * <http://www.w3.org/2006/time#DateTimeDescription> <http://www.w3.org/2000/01/rdf-schema#label> "Date-Time description"@en .
                     * <http://www.w3.org/2006/time#hasTRS> <http://www.w3.org/2000/01/rdf-schema#label> "Temporal reference system used"@en .
                     * <http://www.w3.org/2006/time#unitType> <http://www.w3.org/2000/01/rdf-schema#label> "temporal unit type"@en .
                     * <http://www.w3.org/2006/time#year> <http://www.w3.org/2000/01/rdf-schema#label> "Year"@en .
                     * <http://www.w3.org/2006/time#month> <http://www.w3.org/2000/01/rdf-schema#label> "Month"@en .
                     * <http://www.w3.org/2006/time#day> <http://www.w3.org/2000/01/rdf-schema#label> "Day"@en .
                     * <http://www.w3.org/2006/time#unitYear> <http://www.w3.org/2000/01/rdf-schema#label> "Year (unit of temporal duration)"@en .
                     * <http://www.w3.org/2006/time#unitMonth> <http://www.w3.org/2000/01/rdf-schema#label> "Month (unit of temporal duration)"@en .
                     * <http://www.w3.org/2006/time#unitDay> <http://www.w3.org/2000/01/rdf-schema#label> "Day (unit of temporal duration)"@en .
                     *
                     * <http://www.opengis.net/def/uom/ISO-8601/0/Gregorian> <http://www.w3.org/2000/01/rdf-schema#label> "Gregorian Calendar"@en .
                     * <https://d-nb.info/gnd/4318310-4> <http://www.w3.org/2000/01/rdf-schema#label> "Julian Calendar"@en .
                     */
                    turtles.add("<" + RDF.getUrl() + "type> <" + RDFS.getUrl() + "label> \"has type\"@en .");
                    turtles.add("<" + RDFS.getUrl() + "label> <" + RDFS.getUrl() + "label> \"has label\"@en .");
                    turtles.add("<" + OWL.getUrl() + "sameAs> <" + RDFS.getUrl() + "label> \"same as\"@en .");


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
