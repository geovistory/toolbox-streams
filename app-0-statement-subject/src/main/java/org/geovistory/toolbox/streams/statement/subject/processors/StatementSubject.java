package org.geovistory.toolbox.streams.statement.subject.processors;

import dev.information.statement.Value;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.NodeKey;
import org.geovistory.toolbox.streams.avro.NodeValue;
import org.geovistory.toolbox.streams.avro.StatementEnrichedValue;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.statement.subject.DbTopicNames;
import org.geovistory.toolbox.streams.statement.subject.Env;
import org.geovistory.toolbox.streams.statement.subject.RegisterInputTopic;


public class StatementSubject {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }


    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerInputTopic = new RegisterInputTopic(builder);

        addProcessors(
                registerInputTopic.infStatementTable(),
                registerInputTopic.nodeTable()
        );

        return builder.build();
    }

    public static void addProcessors(
            KTable<dev.information.statement.Key, Value> infStatementTable,
            KTable<NodeKey, NodeValue> nodeTable
    ) {

        var avroSerdes = new ConfluentAvroSerdes();


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
                TableJoined.as(output.TOPICS.statement_with_subject + "-fk-join"),
                Materialized.<dev.information.statement.Key, StatementEnrichedValue, KeyValueStore<Bytes, byte[]>>as(output.TOPICS.statement_with_subject)
                        .withKeySerde(avroSerdes.InfStatementKey())
                        .withValueSerde(avroSerdes.StatementEnrichedValue())
        );


        var stream = statementJoinedWithSubjectTable.toStream(
                Named.as(output.TOPICS.statement_with_subject + "-to-stream")
        );


        stream.to(
                output.TOPICS.statement_with_subject,
                Produced.with(avroSerdes.InfStatementKey(), avroSerdes.StatementEnrichedValue())
                        .withName(output.TOPICS.statement_with_subject + "-producer")
        );
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

    public enum input {
        TOPICS;
        public final String inf_statement = DbTopicNames.inf_statement.getName();
        public final String nodes = Env.INSTANCE.TOPIC_NODES;

    }


    public enum output {
        TOPICS;
        public final String statement_with_subject = Utils.tsPrefixed("statement_with_subject");

    }

}
