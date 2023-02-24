package org.geovistory.toolbox.streams.entity.label.processsors.project;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectEntityValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityVisibilityValue;
import org.geovistory.toolbox.streams.entity.label.RegisterInnerTopic;
import org.geovistory.toolbox.streams.entity.label.processsors.base.ProjectEntityVisibility;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.ProjectedTableRegistrar;
import org.geovistory.toolbox.streams.lib.Utils;


public class ProjectEntity {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var innerTopic = new RegisterInnerTopic(builder);

        return addProcessors(
                builder,
                innerTopic.projectEntityVisibilityStream()
        ).builder().build();
    }

    public static ProjectEntityReturnValue addProcessors(
            StreamsBuilder builder,
            KStream<ProjectEntityKey, ProjectEntityVisibilityValue> projectEntityVisibilityStream) {

        var avroSerdes = new ConfluentAvroSerdes();

        var projector = new ProjectEntityProjector(builder, projectEntityVisibilityStream, avroSerdes);

        /* SINK PROCESSORS */
        projector.addSink();

        return new ProjectEntityReturnValue(builder, projector.kStream);

    }


    public enum input {
        TOPICS;
        public final String project_entity_visibility = ProjectEntityVisibility.output.TOPICS.project_entity_visibility;
    }


    public enum output {
        TOPICS;
        public final String project_entity = Utils.tsPrefixed("project_entity");
    }


    public static class ProjectEntityProjector extends ProjectedTableRegistrar<
            ProjectEntityKey,
            ProjectEntityVisibilityValue,
            ProjectEntityKey,
            ProjectEntityValue
            > {
        public ProjectEntityProjector(
                StreamsBuilder builder,
                KStream<ProjectEntityKey, ProjectEntityVisibilityValue> inputStream,
                ConfluentAvroSerdes avroSerdes
        ) {
            super(
                    builder,
                    inputStream,
                    // prefix for outputs
                    output.TOPICS.project_entity,
                    (key, value) -> KeyValue.pair(
                            key,
                            ProjectEntityValue.newBuilder()
                                    .setProjectId(value.getProjectId())
                                    .setEntityId(value.getEntityId())
                                    .setClassId(value.getClassId())
                                    .setDeleted$1(value.getDeleted$1())
                                    .build()
                    ),
                    avroSerdes.ProjectEntityKey(),
                    avroSerdes.ProjectEntityValue()
            );
        }
    }

}
