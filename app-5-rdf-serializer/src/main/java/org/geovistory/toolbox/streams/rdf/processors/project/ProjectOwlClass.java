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

import static org.geovistory.toolbox.streams.lib.UrlPrefixes.*;


@ApplicationScoped
public class ProjectOwlClass {


    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;


    @Inject
    OutputTopicNames outputTopicNames;

    public ProjectOwlClass(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {
         addProcessors(
                 registerInputTopic.projectClassLabelStream()
        );
    }

    public  ProjectRdfReturnValue addProcessors(
            KStream<ProjectClassLabelKey, ProjectClassLabelValue> projectClassLabelStream
    ) {

        /* STREAM PROCESSORS */
        // 2)

        var s = projectClassLabelStream.flatMap(
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
                var classLiteralType = (value.getLanguageIso() == null) ? "^^<"+XSD+"string>" : "@"+ value.getLanguageIso();

                turtles.add("<" + ONTOME_CLASS.getUrl() + classId + "> a <" + OWL +"Class>");
                turtles.add("<" + ONTOME_CLASS.getUrl() + classId + "> <" + RDFS +"label> "+ classLabel.replaceAll("[\\\\\"]", "\\$0") + classLiteralType +" .");

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


}
