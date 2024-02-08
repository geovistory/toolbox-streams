package org.geovistory.toolbox.streams.statement.object.processors;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.NodeKey;
import org.geovistory.toolbox.streams.avro.NodeValue;
import org.geovistory.toolbox.streams.avro.StatementEnrichedValue;
import org.geovistory.toolbox.streams.avro.TextValue;
import org.geovistory.toolbox.streams.lib.IdenticalRecordsFilterSupplier;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.statement.object.AvroSerdes;
import org.geovistory.toolbox.streams.statement.object.BuilderSingleton;
import org.geovistory.toolbox.streams.statement.object.RegisterInputTopic;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Objects;


@ApplicationScoped
public class StatementObject {


    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;
    @Inject
    public BuilderSingleton builderSingleton;
    @ConfigProperty(name = "ts.output.topic.name.prefix", defaultValue = "")
    public String outPrefix;
    @ConfigProperty(name = "create.output.for.postgres", defaultValue = "false")
    public String createOutputForPostgres;

    public StatementObject(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
    }

    public void addProcessorsStandalone() {
        addProcessors(
                registerInputTopic.statementWithSubjectTable(),
                registerInputTopic.nodeTable()
        );
    }

    public void addProcessors(
            KTable<ts.information.statement.Key, StatementEnrichedValue> statementWithSubject,
            KTable<NodeKey, NodeValue> nodeTable
    ) {

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
                Materialized.<ts.information.statement.Key, StatementEnrichedValue, KeyValueStore<Bytes, byte[]>>as("statement_with_object")
                        .withKeySerde(avroSerdes.InfStatementKey())
                        .withValueSerde(avroSerdes.StatementEnrichedValue())
        );

        var stream = statementJoinedWithObjectTable.toStream(
                        Named.as("statement_with_object" + "-to-stream")
                )
                .transform(new IdenticalRecordsFilterSupplier<>("statement_enriched_suppress_duplicates",
                        avroSerdes.InfStatementKey(), avroSerdes.StatementEnrichedValue()
                ));

        Map<String, KStream<ts.information.statement.Key, StatementEnrichedValue>> branches =
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
                outStatementWithEntity(),
                Produced.with(avroSerdes.InfStatementKey(), avroSerdes.StatementEnrichedValue())
                        .withName(outStatementWithEntity() + "-producer")
        );
        l.to(
                outStatementWithLiteral(),
                Produced.with(avroSerdes.InfStatementKey(), avroSerdes.StatementEnrichedValue())
                        .withName(outStatementWithLiteral() + "-producer")
        );
        o.to(
                outStatementOther(),
                Produced.with(avroSerdes.InfStatementKey(), avroSerdes.StatementEnrichedValue())
                        .withName(outStatementOther() + "-producer")
        );

        // if "true" the app creates a topic "statement_enriched_flat" that can be sinked to postgres
        if (Objects.equals(createOutputForPostgres, "true")) {

            builderSingleton.builder.stream(
                            outStatementWithLiteral(),
                            Consumed.with(avroSerdes.InfStatementKey(), avroSerdes.StatementEnrichedValue())
                                    .withName(outStatementWithLiteral() + "-consumer")
                    )
                    .mapValues((readOnlyKey, value) -> TextValue.newBuilder().setText(
                            value.toString()
                    ).build())
                    .to(
                            outStatementEnrichedFlat(),
                            Produced.with(avroSerdes.InfStatementKey(), avroSerdes.TextValue())
                                    .withName(outStatementEnrichedFlat() + "-literal-producer")
                    );

            builderSingleton.builder.stream(
                            outStatementWithEntity(),
                            Consumed.with(avroSerdes.InfStatementKey(), avroSerdes.StatementEnrichedValue())
                                    .withName(outStatementWithEntity() + "-consumer")
                    )
                    .mapValues((readOnlyKey, value) -> TextValue.newBuilder().setText(
                            value.toString()
                    ).build())
                    .to(
                            outStatementEnrichedFlat(),
                            Produced.with(avroSerdes.InfStatementKey(), avroSerdes.TextValue())
                                    .withName(outStatementEnrichedFlat() + "-entity-producer")
                    );
        }


    }


    public String inStatementWithSubject() {
        return registerInputTopic.tsTopicStatementWithSubject;
    }

    public String inNodes() {
        return registerInputTopic.tsTopicNodes;
    }

    public String outStatementWithEntity() {
        return Utils.prefixedOut(outPrefix, "statement_with_entity");
    }

    public String outStatementWithLiteral() {
        return Utils.prefixedOut(outPrefix, "statement_with_literal");
    }

    public String outStatementOther() {
        return Utils.prefixedOut(outPrefix, "statement_other");
    }

    public String outStatementEnrichedFlat() {
        return Utils.prefixedOut(outPrefix, "statement_enriched_flat");
    }


}
