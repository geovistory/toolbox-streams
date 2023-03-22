package org.geovistory.toolbox.streams.rdf.processors.project;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.rdf.RegisterInputTopic;

import java.util.LinkedList;
import java.util.List;

import static org.geovistory.toolbox.streams.lib.UrlPrefixes.*;


public class ProjectStatementToUri {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    /**
     * Only used for the unit tests
     */
    public static Topology buildStandalone(StreamsBuilder builder) {
        var inputTopic = new RegisterInputTopic(builder);

        return addProcessors(
                builder,
                inputTopic.projectStatementWithEntityStream()
        ).builder().build();
    }

    public static ProjectRdfReturnValue addProcessors(
            StreamsBuilder builder,
            KStream<ProjectStatementKey, ProjectStatementValue> projectStatementWithEntityStream
    ) {

        var avroSerdes = new ConfluentAvroSerdes();


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
                        .setTurtle("<"+ GEOVISTORY_RESOURCE.getUrl()+subjectId+"> <"+ ONTOME_PROPERTY.getUrl() +propertyId+"> <"+ GEOVISTORY_RESOURCE.getUrl()+objectId+">")
                        .build();
                result.add(KeyValue.pair(k, v));

                var ki = ProjectRdfKey.newBuilder()
                        .setProjectId(value.getProjectId())
                        .setTurtle("<"+ GEOVISTORY_RESOURCE.getUrl()+objectId+"> <"+ ONTOME_PROPERTY.getUrl() +propertyId+"i> <"+ GEOVISTORY_RESOURCE.getUrl()+subjectId+">")
                        .build();
                result.add(KeyValue.pair(ki, v));

                return result;
            }
        );
        /* SINK PROCESSORS */

        s.to(output.TOPICS.project_rdf,
                Produced.with(avroSerdes.ProjectRdfKey(), avroSerdes.ProjectRdfValue())
                        .withName(output.TOPICS.project_rdf + "-producer")
        );


        return new ProjectRdfReturnValue(builder, s);

    }

    public enum output {
        TOPICS;
        public final String project_rdf = Utils.tsPrefixed("project_rdf");
    }

}
