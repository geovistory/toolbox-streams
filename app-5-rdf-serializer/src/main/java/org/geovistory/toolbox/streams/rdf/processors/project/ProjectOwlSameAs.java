package org.geovistory.toolbox.streams.rdf.processors.project;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.analysis.statements.avro.AnalysisStatementKey;
import org.geovistory.toolbox.streams.analysis.statements.avro.AnalysisStatementValue;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.rdf.AvroSerdes;
import org.geovistory.toolbox.streams.rdf.OutputTopicNames;
import org.geovistory.toolbox.streams.rdf.RegisterInputTopic;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.geovistory.toolbox.streams.lib.UrlPrefixes.*;
import static org.geovistory.toolbox.streams.lib.UrlPrefixes.XSD;


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

        /*
         * 2) KTable of left statement
         * 2a) FlatMap the stream to List<KeyValue<ProjectEntityKey, Boolean>>
         */

        var s1 = projectStatementWithEntityStream.flatMap(
                (key, value) -> {
                    List<KeyValue<ProjectEntityKey, TextWithDeleteValue>> result = new LinkedList<>();

                    if (value.getStatement().getPropertyId() == 1943) {
                        var k = new ProjectEntityKey().newBuilder().setProjectId(key.getProjectId()).setEntityId(value.getStatement().getObjectId()).build();
                        var textWithDeleteValue = TextWithDeleteValue.newBuilder()
                                .setText(value.getStatement().getSubjectId())
                                .setDeleted(value.getDeleted$1())
                                .build();

                        result.add(KeyValue.pair(k, textWithDeleteValue));

                    }

                    return result;
                }
        );

        // 2b) ToTable: Convert the stream to a table
        var table1 = s1.toTable(
                Named.as("project_statement_with_entity_stream_filtered"),
                Materialized.with(avroSerdes.ProjectEntityKey(), avroSerdes.TextWithDeleteValue())
        );

        /*
         * 3) KTable of right statement
         * 3a) FlatMap the stream by List<KeyValue<ProjectEntityKey, TextWithDeleteValue>>
         */
        var s2 = projectStatementWithLiteralStream.flatMap(
                (key, value) -> {
                    List<KeyValue<ProjectEntityKey, TextWithDeleteValue>> result = new LinkedList<>();

                    if (value.getStatement().getPropertyId() == 1843 && value.getStatement().getObject().getAppellation().getString().matches("^[a-z](?:[-a-z0-9\\+\\.])*:(?:\\/\\/(?:(?:%[0-9a-f][0-9a-f]|[-a-z0-9\\._~!\\$&''\\(\\)\\*\\+,;=:@])|[\\/\\?])*)?")) {
                        var k = new ProjectEntityKey().newBuilder().setProjectId(key.getProjectId()).setEntityId(Integer.toString(value.getStatementId())).build();

                        var textWithDeleteValue = TextWithDeleteValue.newBuilder()
                                .setText(value.getStatement().getSubjectId())
                                .setDeleted(value.getDeleted$1())
                                .build();
                        result.add(KeyValue.pair(k, textWithDeleteValue));
                    }

                    return result;
                }
        );

        // 3b) ToTable: Convert the stream to a table
        var table2 = s2.toTable(
                Named.as("project_statement_with_literal_stream_filtered"),
                Materialized.with(avroSerdes.ProjectEntityKey(), avroSerdes.TextWithDeleteValue())
        );
        /*
         * 4) Join and produce turtle
         * 4a) Join the KTables (KTable-KTable Equi-Join), creating a ProjectRdfRecord
         */

        var joinTable = table1.join(table2, (TextWithDeleteValue table1Value, TextWithDeleteValue table2Value) -> {
                    TextWithDeleteValue[] a = {table1Value, table2Value};
                    return a;
                }
        );

        var mapped = joinTable.toStream().map((key, value) -> {

            var k = ProjectRdfKey.newBuilder().setProjectId(key.getProjectId()).setTurtle("").build();
            var v = ProjectRdfValue.newBuilder().setOperation(Operation.insert).build();

            return KeyValue.pair(k, v);

        });



        /* 5) SINK PROCESSORS */

        mapped.to(outputTopicNames.projectRdf(),
                Produced.with(avroSerdes.ProjectRdfKey(), avroSerdes.ProjectRdfValue())
                        .withName(outputTopicNames.projectRdf() + "-owl-same-as-producer")
        );

        return new ProjectRdfReturnValue(mapped);

    }


}