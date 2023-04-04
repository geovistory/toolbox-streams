package org.geovistory.toolbox.streams.statement.subject.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import dev.information.statement.Value;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.NodeKey;
import org.geovistory.toolbox.streams.avro.NodeValue;
import org.geovistory.toolbox.streams.avro.StatementEnrichedValue;
import org.geovistory.toolbox.streams.avro.TextValue;
import org.geovistory.toolbox.streams.lib.JsonStringifier;
import org.geovistory.toolbox.streams.lib.TopicNameEnum;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.statement.subject.AvroSerdes;
import org.geovistory.toolbox.streams.statement.subject.BuilderSingleton;
import org.geovistory.toolbox.streams.statement.subject.RegisterInputTopic;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.Objects;

@ApplicationScoped
public class StatementSubject {


    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;

    @Inject
    public BuilderSingleton builderSingleton;

    @ConfigProperty(name = "ts.input.topic.name.prefix", defaultValue = "")
    String inPrefix;
    @ConfigProperty(name = "ts.output.topic.name.prefix", defaultValue = "")
    public String outPrefix;
    @ConfigProperty(name = "create.output.for.postgres", defaultValue = "false")
    public String createOutputForPostgres;

    public StatementSubject(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
    }

    public void addProcessorsStandalone() {

        addProcessors(
                registerInputTopic.infStatementTable(),
                registerInputTopic.nodeTable()
        );

    }

    public void addProcessors(
            KTable<dev.information.statement.Key, Value> infStatementTable,
            KTable<NodeKey, NodeValue> nodeTable
    ) {

        // join subject
        var statementJoinedWithSubjectTable = infStatementTable.join(
                nodeTable,
                value -> NodeKey.newBuilder()
                        .setId(getSubjectStringId(value))
                        .build(),
                (statement, subject) -> {
                    var v = StatementEnrichedValue.newBuilder()
                            .setSubjectId(getSubjectStringId(statement))
                            .setPropertyId(statement.getFkProperty())
                            .setObjectId(getObjectStringId(statement))
                            .setDeleted$1(Utils.stringIsEqualTrue(statement.getDeleted$1()));
                    if (subject != null) {
                        v.setSubjectLabel(subject.getLabel())
                                .setSubject(subject)
                                .setSubjectClassId(subject.getClassId());
                    }
                    return v.build();
                },
                TableJoined.as(outStatementWithSubject() + "-fk-join"),
                Materialized.<dev.information.statement.Key, StatementEnrichedValue, KeyValueStore<Bytes, byte[]>>as(outStatementWithSubject())
                        .withKeySerde(avroSerdes.InfStatementKey())
                        .withValueSerde(avroSerdes.StatementEnrichedValue())
        );


        var stream = statementJoinedWithSubjectTable.toStream(
                Named.as(outStatementWithSubject() + "-to-stream")
        );


        stream.to(
                outStatementWithSubject(),
                Produced.with(avroSerdes.InfStatementKey(), avroSerdes.StatementEnrichedValue())
                        .withName(outStatementWithSubject() + "-producer")
        );

        // if "true" the app creates a topic "statement_enriched_flat" that can be sinked to postgres
        if (Objects.equals(createOutputForPostgres, "true")) {
            var mapper = JsonStringifier.getMapperIgnoringNulls();
            builderSingleton.builder.stream(
                            outStatementWithSubject(),
                            Consumed.with(avroSerdes.InfStatementKey(), avroSerdes.StatementEnrichedValue())
                                    .withName(outStatementWithSubject() + "-consumer")
                    )
                    .mapValues((readOnlyKey, value) -> {
                        try {
                            return TextValue.newBuilder().setText(
                                    mapper.writeValueAsString(value)
                            ).build();
                        } catch (JsonProcessingException ex) {
                            throw new RuntimeException(ex);
                        }
                    })
                    .to(
                            outStatementWithSubjectFlat(),
                            Produced.with(avroSerdes.InfStatementKey(), avroSerdes.TextValue())
                                    .withName(outStatementWithSubjectFlat() + "-producer")
                    );
        }
    }

    /**
     * Returns a string object id for statement prefixed
     * with one letter for the postgres schema name:
     * - "i" for information
     * - "d" for data
     * - "t" for table
     *
     * @param value statement
     * @return e.g. "i2134123" or "t232342"
     */
    private static String getObjectStringId(Value value) {
        String id = "";
        if (value.getFkObjectInfo() > 0) id = "i" + value.getFkObjectInfo();
        else if (value.getFkObjectTablesCell() > 0) id = "t" + value.getFkObjectTablesCell();
        else if (value.getFkObjectData() > 0) id = "d" + value.getFkObjectData();
        return id;
    }

    /**
     * Returns a string object id for statement prefixed
     * with one letter for the postgres schema name:
     * - "i" for information
     * - "d" for data
     * - "t" for table
     *
     * @param value statement
     * @return e.g. "i2134123" or "t232342"
     */
    private static String getSubjectStringId(Value value) {
        String id = "";
        if (value.getFkSubjectInfo() > 0) id = "i" + value.getFkSubjectInfo();
        else if (value.getFkSubjectTablesCell() > 0) id = "t" + value.getFkSubjectTablesCell();
        else if (value.getFkSubjectData() > 0) id = "d" + value.getFkSubjectData();
        return id;
    }


    public String inInfStatement() {
        return Utils.prefixedIn(inPrefix, TopicNameEnum.inf_statement.getValue());
    }

    public String inNodes() {
        return registerInputTopic.tsTopicNodes;
    }

    public String outStatementWithSubject() {
        return Utils.prefixedOut(outPrefix, "statement_with_subject");
    }

    public String outStatementWithSubjectFlat() {
        return Utils.prefixedOut(outPrefix, "statement_with_subject_flat");
    }


}
