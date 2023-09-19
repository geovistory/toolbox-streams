package org.geovistory.toolbox.streams.entity.label.processsors.community;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.geovistory.toolbox.streams.avro.ProjectStatementKey;
import org.geovistory.toolbox.streams.avro.ProjectStatementValue;
import org.geovistory.toolbox.streams.entity.label.AvroSerdes;
import org.geovistory.toolbox.streams.entity.label.OutputTopicNames;
import org.geovistory.toolbox.streams.entity.label.RegisterInnerTopic;
import org.geovistory.toolbox.streams.entity.label.RegisterInputTopic;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;


@ApplicationScoped
public class CommunityToolboxStatementWithEntity {

    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;
    @Inject
    RegisterInnerTopic registerInnerTopic;

    @Inject
    OutputTopicNames outputTopicNames;

    public CommunityToolboxStatementWithEntity(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, RegisterInnerTopic registerInnerTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.registerInnerTopic = registerInnerTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {


        addProcessors(
                registerInnerTopic.projectStatementWithEntityStream()
        );
    }

    public CommunityToolboxStatementReturnValue addProcessors(
            KStream<ProjectStatementKey, ProjectStatementValue> projectStatementWithEntityStream) {

        var result = projectStatementWithEntityStream
                .transform(new CommunityToolboxStatementCounterSupplier("community_toolbox_statement_with_entity_counter", avroSerdes));
        result.to(outputTopicNames.communityToolboxStatementWithEntity(),
                Produced.with(avroSerdes.CommunityStatementKey(), avroSerdes.CommunityStatementValue())
                        .withName(outputTopicNames.communityToolboxStatementWithEntity() + "-producer")
        );

        return new CommunityToolboxStatementReturnValue(result);

    }


    public enum inner {
        TOPICS
    }

}
