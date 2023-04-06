package org.geovistory.toolbox.streams.statement.object.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.NodeKey;
import org.geovistory.toolbox.streams.avro.NodeValue;
import org.geovistory.toolbox.streams.avro.StatementEnrichedValue;
import org.geovistory.toolbox.streams.avro.TextValue;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.IdenticalRecordsFilterSupplier;
import org.geovistory.toolbox.streams.lib.JsonStringifier;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.statement.object.Env;
import org.geovistory.toolbox.streams.statement.object.RegisterInputTopic;

import java.util.Map;
import java.util.Objects;


public class StatementObject {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }


    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerInputTopic = new RegisterInputTopic(builder);

        addProcessors(
                registerInputTopic.statementWithSubjectTable(),
                registerInputTopic.nodeTable()
        );

        return builder.build();
    }

    public static void addProcessors(
            KTable<dev.information.statement.Key, StatementEnrichedValue> statementWithSubject,
            KTable<NodeKey, NodeValue> nodeTable


    ) {

        var avroSerdes = new ConfluentAvroSerdes();


        // join object
        var statementJoinedWithObjectTable = statementWithSubject.join(
                nodeTable,
                value -> NodeKey.newBuilder()
                        .setId(value.getObjectId())
                        .build(),
                (statementEnrichedValue, object) -> {
                    if (object != null) {
                        statementEnrichedValue.setObjectLabel(object.getLabel());
                        statementEnrichedValue.setObject(object);
                        statementEnrichedValue.setObjectClassId(object.getClassId());
                    }
                    return statementEnrichedValue;
                },
                TableJoined.as("statement_with_object" + "-fk-join"),
                Materialized.<dev.information.statement.Key, StatementEnrichedValue, KeyValueStore<Bytes, byte[]>>as("statement_with_object")
                        .withKeySerde(avroSerdes.InfStatementKey())
                        .withValueSerde(avroSerdes.StatementEnrichedValue())
        );

        var stream = statementJoinedWithObjectTable.toStream(
                        Named.as("statement_with_object" + "-to-stream")
                )
                .transform(new IdenticalRecordsFilterSupplier<>("statement_enriched_suppress_duplicates",
                        avroSerdes.InfStatementKey(), avroSerdes.StatementEnrichedValue()
                ));

        Map<String, KStream<dev.information.statement.Key, StatementEnrichedValue>> branches =
                stream.split(Named.as("Branch-"))
                        .branch((key, value) -> value != null && value.getObject().getEntity() != null,  /* first predicate  */
                                Branched.as("Entity"))
                        .branch((key, value) -> value != null && value.getObject().getEntity() == null,  /* second predicate */
                                Branched.as("Literal"))
                        .defaultBranch(Branched.as("Other"));          /* default branch */

        var e = branches.get("Branch-Entity"); // contains all records whose objects are entities
        var l = branches.get("Branch-Literal"); // contains all records whose objects are literals
        var o = branches.get("Branch-Other"); // contains all other records

        e.to(
                output.TOPICS.statement_with_entity,
                Produced.with(avroSerdes.InfStatementKey(), avroSerdes.StatementEnrichedValue())
                        .withName(output.TOPICS.statement_with_entity + "-producer")
        );
        l.to(
                output.TOPICS.statement_with_literal,
                Produced.with(avroSerdes.InfStatementKey(), avroSerdes.StatementEnrichedValue())
                        .withName(output.TOPICS.statement_with_literal + "-producer")
        );
        o.to(
                output.TOPICS.statement_other,
                Produced.with(avroSerdes.InfStatementKey(), avroSerdes.StatementEnrichedValue())
                        .withName(output.TOPICS.statement_other + "-producer")
        );

        // if "true" the app creates a topic "statement_enriched_flat" that can be sinked to postgres
        if (Objects.equals(Env.INSTANCE.CREATE_OUTPUT_FOR_POSTGRES, "true")) {
            var mapper =  JsonStringifier.getMapperIgnoringNulls();
            stream.mapValues((readOnlyKey, value) -> {
                        try {
                            return TextValue.newBuilder().setText(
                                            mapper.writeValueAsString(value)
                                    ).build();
                        } catch (JsonProcessingException ex) {
                            throw new RuntimeException(ex);
                        }
                    })
                    .to(
                            output.TOPICS.statement_enriched_flat,
                            Produced.with(avroSerdes.InfStatementKey(), avroSerdes.TextValue())
                                    .withName(output.TOPICS.statement_enriched_flat + "-producer")
                    );
        }


    }

    public enum input {
        TOPICS;
        public final String statement_with_subject = Env.INSTANCE.TOPIC_STATEMENT_WITH_SUBJECT;
        public final String nodes = Env.INSTANCE.TOPIC_NODES;
    }


    public enum output {
        TOPICS;
        public final String statement_with_entity = Utils.tsPrefixed("statement_with_entity");
        public final String statement_with_literal = Utils.tsPrefixed("statement_with_literal");
        public final String statement_other = Utils.tsPrefixed("statement_other");

        public final String statement_enriched_flat = Utils.tsPrefixed("statement_enriched_flat");

    }

}
