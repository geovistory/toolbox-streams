
package org.geovistory.toolbox.streams.analysis.statements;

import org.apache.kafka.streams.kstream.KStream;
import org.geovistory.toolbox.streams.avro.ProjectStatementKey;
import org.geovistory.toolbox.streams.avro.ProjectStatementValue;
import org.geovistory.toolbox.streams.lib.TsRegisterInputTopic;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

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


    public KStream<ProjectStatementKey, ProjectStatementValue> projectStatementWithLiteral() {
        return getStream(
                builderSingleton.builder,
                inputTopicNames.getProjectStatementWithLiteral(),
                avroSerdes.ProjectStatementKey(),
                avroSerdes.ProjectStatementValue()
        );
    }

    public KStream<ProjectStatementKey, ProjectStatementValue> projectStatementWithEntity() {
        return getStream(
                builderSingleton.builder,
                inputTopicNames.getProjectStatementWithEntity(),
                avroSerdes.ProjectStatementKey(),
                avroSerdes.ProjectStatementValue()
        );
    }
}
