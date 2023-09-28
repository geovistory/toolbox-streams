package org.geovistory.toolbox.streams.rdf.processors.project;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
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

    public  ProjectRdfReturnValue addProcessors(
            KStream<ProjectStatementKey, ProjectStatementValue> projectStatementWithLiteralStream,
            KStream<ProjectStatementKey, ProjectStatementValue> projectStatementWithEntityStream,
            KStream<OntomePropertyLabelKey, OntomePropertyLabelValue> projectClassLabelStream
    ) {

        /* STREAM PROCESSORS */

        // 2) project_statement_with_literal group by ProjectOwlPropertyKey
        var groupedStatementWithLiteral = projectStatementWithLiteralStream.groupBy(
                (key, value) ->
                        ProjectOwlPropertyKey.newBuilder()
                                .setProjectId(key.getProjectId())
                                .setPropertyId("p123")
                                .build(),
                Grouped.with(
                        avroSerdes.ProjectOwlPropertyKey(), Serdes.String()
                ));

        KGroupedStream<ProjectOwlPropertyKey, String> groupedStream = projectStatementWithLiteralStream.groupBy(
                (key, value) ->
                        ProjectOwlPropertyKey.newBuilder()
                        .setProjectId(key.getProjectId())
                        .setPropertyId("p123")
                        .build(),
                Grouped.with(
                        avroSerdes.ProjectOwlPropertyKey(), /* key (note: type was modified) */
                        Serdes.String())  /* value */
        );

        var s = projectClassLabelStream.flatMap(
                (key, value) -> {
                    List<KeyValue<ProjectRdfKey, ProjectRdfValue>> result = new LinkedList<>();

                    //value of operation

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