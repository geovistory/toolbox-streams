package org.geovistory.toolbox.streams.entity.label.processsors.project;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;
import org.geovistory.toolbox.streams.avro.ProjectEntityValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityVisibilityValue;
import org.geovistory.toolbox.streams.entity.label.*;
import org.geovistory.toolbox.streams.lib.ProjectedTableRegistrar;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;


@ApplicationScoped
public class ProjectEntity {


    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;
    @Inject
    RegisterInnerTopic registerInnerTopic;

    @Inject
    OutputTopicNames outputTopicNames;

    @Inject
    BuilderSingleton builderSingleton;

    public ProjectEntity(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, RegisterInnerTopic registerInnerTopic, OutputTopicNames outputTopicNames, BuilderSingleton builderSingleton) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.registerInnerTopic = registerInnerTopic;
        this.outputTopicNames = outputTopicNames;
        this.builderSingleton = builderSingleton;
    }

    public void addProcessorsStandalone() {

        addProcessors(
                registerInnerTopic.projectEntityVisibilityStream()
        );
    }

    public ProjectEntityReturnValue addProcessors(
            KStream<ProjectEntityKey, ProjectEntityVisibilityValue> projectEntityVisibilityStream) {


        var projector = new ProjectEntityProjector(
                builderSingleton.builder,
                projectEntityVisibilityStream,
                avroSerdes,
                outputTopicNames.projectEntity()
        );

        /* SINK PROCESSORS */
        projector.addSink();

        return new ProjectEntityReturnValue(projector.kStream);

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
                AvroSerdes avroSerdes,
                String outputTopic
        ) {
            super(
                    builder,
                    inputStream,
                    // prefix for outputs
                    outputTopic,
                    (key, value) -> KeyValue.pair(
                            key,
                            ProjectEntityValue.newBuilder()
                                    .setProjectId(key.getProjectId())
                                    .setEntityId(key.getEntityId())
                                    .setClassId(value == null ? 0 : value.getClassId())
                                    .setDeleted$1(value == null || value.getDeleted$1())
                                    .build()
                    ),
                    avroSerdes.ProjectEntityKey(),
                    avroSerdes.ProjectEntityValue()
            );
        }
    }

}
