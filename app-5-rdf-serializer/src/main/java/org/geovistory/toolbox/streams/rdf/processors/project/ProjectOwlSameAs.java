package org.geovistory.toolbox.streams.rdf.processors.project;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.rdf.AvroSerdes;
import org.geovistory.toolbox.streams.rdf.OutputTopicNames;
import org.geovistory.toolbox.streams.rdf.RegisterInputTopic;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;

import static org.geovistory.toolbox.streams.lib.UrlPrefixes.GEOVISTORY_RESOURCE;
import static org.geovistory.toolbox.streams.lib.UrlPrefixes.ONTOME_PROPERTY;


@ApplicationScoped
public class ProjectOwlSameAs {


    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;


    @Inject
    OutputTopicNames outputTopicNames;

    public ProjectOwlSameAs(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {
        addProcessors(
                registerInputTopic.projectStatementWithEntityStream(),
                registerInputTopic.projectStatementWithLiteralStream()
        );
    }

    public ProjectRdfReturnValue addProcessors(
            KStream<ProjectStatementKey, ProjectStatementValue> projectStatementWithEntityStream,
            KStream<ProjectStatementKey, ProjectStatementValue> projectStatementWithLiteralStream
    ) {

        /* STREAM PROCESSORS */
        // 2a) FlatMap projectStatementWithEntity to List<KeyValue<ProjectEntityKey, Boolean>>

        var s1 = projectStatementWithEntityStream.flatMap(
                (key, value) -> {
                    List<KeyValue<ProjectEntityKey, Boolean>> result = new LinkedList<>();

                    if (value.getStatement().getPropertyId() == 1943) {
                        var k = new ProjectEntityKey().newBuilder().setProjectId(key.getProjectId()).setEntityId(Integer.toString(value.getStatementId())).build();
                        var v = value.getDeleted$1();

                        result.add(KeyValue.pair(k, v));

                    }

                    return result;
                }
        );

        var table1 = s1.toTable(

        );

        var s2 = projectStatementWithEntityStream.flatMap(
                (key, value) -> {
                    List<KeyValue<ProjectEntityKey, TextWithDeleteValue>> result = new LinkedList<>();

                    if (value.getStatement().getPropertyId() == 1843 && value.getStatement().getObject().getAppellation().getString().matches("^[a-z](?:[-a-z0-9\\+\\.])*:(?:\\/\\/(?:(?:%[0-9a-f][0-9a-f]|[-a-z0-9\\._~!\\$&''\\(\\)\\*\\+,;=:@])|[\\/\\?])*)?")) {
                        var k = new ProjectEntityKey().newBuilder().setProjectId(key.getProjectId()).setEntityId(Integer.toString(value.getStatementId())).build();

                        var textWithDeleteValue = TextWithDeleteValue.newBuilder()
                                .setText(value.getStatement().getSubjectId())
                                .setDeleted$1(value.getDeleted$1())
                                .build();
                        result.add(KeyValue.pair(k, textWithDeleteValue));
                    }

                    return result;
                }
        );


        var table2 = s2.toTable(

        );
        /* SINK PROCESSORS */



    }


}
