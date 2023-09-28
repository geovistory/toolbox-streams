
package org.geovistory.toolbox.streams.rdf;

import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.TsRegisterInputTopic;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/**
 * This class provides helper methods to register
 * output topics (generated by this app).
 * These helper methods are mainly used for testing.
 */
@ApplicationScoped
public class RegisterInputTopic extends TsRegisterInputTopic {

    @Inject
    AvroSerdes avroSerdes;
    @Inject
    public BuilderSingleton builderSingleton;

    @Inject
    public InputTopicNames inputTopicNames;

    public RegisterInputTopic(AvroSerdes avroSerdes, BuilderSingleton builderSingleton, InputTopicNames inputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.builderSingleton = builderSingleton;
        this.inputTopicNames = inputTopicNames;
    }

    public KStream<ProjectStatementKey, ProjectStatementValue> projectStatementWithEntityStream() {
        return getStream(
                builderSingleton.builder,
                inputTopicNames.getProjectStatementWithEntity(),
                avroSerdes.ProjectStatementKey(),
                avroSerdes.ProjectStatementValue()
        );
    }

    public KStream<ProjectStatementKey, ProjectStatementValue> projectStatementWithLiteralStream() {
        return getStream(
                builderSingleton.builder,
                inputTopicNames.getProjectStatementWithLiteral(),
                avroSerdes.ProjectStatementKey(),
                avroSerdes.ProjectStatementValue()
        );
    }

    public KStream<ProjectClassLabelKey, ProjectClassLabelValue> projectClassLabelStream() {
        return getStream(
                builderSingleton.builder,
                inputTopicNames.getProjectClassLabel(),
                avroSerdes.ProjectClassLabelKey(),
                avroSerdes.ProjectClassLabelValue()
        );
    }

    public KStream<ProjectEntityKey, ProjectEntityLabelValue> projectEntityLabelStream() {
        return getStream(
                builderSingleton.builder,
                inputTopicNames.getProjectEntityLabel(),
                avroSerdes.ProjectEntityKey(),
                avroSerdes.ProjectEntityLabelValue()
        );
    }

    public KStream<dev.projects.project.Key, dev.projects.project.Value> projectStream() {
        return getStream(
                builderSingleton.builder,
                inputTopicNames.getProject(),
                avroSerdes.ProProjectKey(),
                avroSerdes.ProProjectValue()
        );
    }

}
