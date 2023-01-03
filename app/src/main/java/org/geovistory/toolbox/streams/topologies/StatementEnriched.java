package org.geovistory.toolbox.streams.topologies;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.app.DbTopicNames;
import org.geovistory.toolbox.streams.app.RegisterInputTopic;
import org.geovistory.toolbox.streams.avro.LiteralKey;
import org.geovistory.toolbox.streams.avro.LiteralValue;
import org.geovistory.toolbox.streams.avro.StatementEnrichedKey;
import org.geovistory.toolbox.streams.avro.StatementEnrichedValue;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.Utils;


public class StatementEnriched {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    public static Topology buildStandalone(StreamsBuilder builder) {
        var registerInputTopic = new RegisterInputTopic(builder);

        return addProcessors(
                builder,
                registerInputTopic.infStatementTable(),
                registerInputTopic.infLanguageStream(),
                registerInputTopic.infAppellationStream(),
                registerInputTopic.infLangStringStream(),
                registerInputTopic.infPlaceStream(),
                registerInputTopic.infTimePrimitiveStream(),
                registerInputTopic.infDimensionStream()
        ).build();
    }

    public static StreamsBuilder addProcessors(
            StreamsBuilder builder,
            KTable<dev.information.statement.Key, dev.information.statement.Value> infStatementTable,
            KStream<dev.information.language.Key, dev.information.language.Value> infLanguageTable,
            KStream<dev.information.appellation.Key, dev.information.appellation.Value> infAppellationTable,
            KStream<dev.information.lang_string.Key, dev.information.lang_string.Value> infLangStringTable,
            KStream<dev.information.place.Key, dev.information.place.Value> infPlaceTable,
            KStream<dev.information.time_primitive.Key, dev.information.time_primitive.Value> infTimePrimitiveTable,
            KStream<dev.information.dimension.Key, dev.information.dimension.Value> infDimensionTable
    ) {

        var avroSerdes = new ConfluentAvroSerdes();

        // Map languages to literals
        var languageLiterals = infLanguageTable.map((key, value) -> KeyValue.pair(
                LiteralKey.newBuilder().setId(value.getPkEntity()).build(),
                LiteralValue.newBuilder().setId(value.getPkEntity())
                        .setLanguage(value)
                        .setClassId(value.getFkClass())
                        .setLabel(value.getNotes())
                        .build()
        ));


        // Map appellations to literals
        var appellationLiterals = infAppellationTable.map((key, value) -> KeyValue.pair(
                LiteralKey.newBuilder().setId(key.getPkEntity()).build(),
                LiteralValue.newBuilder().setId(key.getPkEntity())
                        .setAppellation(value)
                        .setClassId(value.getFkClass())
                        .setLabel(value.getString())
                        .build()
        ));

        // Map langStrings to literals
        var langStringLiterals = infLangStringTable.map((key, value) -> KeyValue.pair(
                LiteralKey.newBuilder().setId(key.getPkEntity()).build(),
                LiteralValue.newBuilder().setId(key.getPkEntity())
                        .setLangString(value)
                        .setClassId(value.getFkClass())
                        .setLabel(value.getString())
                        .build()
        ));

        // Map places to literals
        var placeLiterals = infPlaceTable.map((key, value) -> {
                    var db = value.getGeoPoint().getWkb().asDoubleBuffer();
                    var x = db.get(0);
                    var y = db.get(1);
                    return KeyValue.pair(
                            LiteralKey.newBuilder().setId(key.getPkEntity()).build(),
                            LiteralValue.newBuilder().setId(key.getPkEntity())
                                    .setPlace(value)
                                    .setClassId(value.getFkClass())
                                    .setLabel(
                                            "WGS84: " + x + "°, " + y + "°"
                                    ).build()
                    );
                }
        );

        // Map timePrimitives to literals
        var timePrimitiveLiterals = infTimePrimitiveTable.map((key, value) -> KeyValue.pair(
                        LiteralKey.newBuilder().setId(key.getPkEntity()).build(),
                        LiteralValue.newBuilder().setId(key.getPkEntity())
                                .setTimePrimitive(value)
                                .setClassId(value.getFkClass())
                                .setLabel("todo")
                                .build()
                )
        );

        // Map dimensions to literals
        var dimensionLiterals = infDimensionTable.map((key, value) -> KeyValue.pair(
                        LiteralKey.newBuilder().setId(key.getPkEntity()).build(),
                        LiteralValue.newBuilder().setId(key.getPkEntity())
                                .setDimension(value)
                                .setClassId(value.getFkClass())
                                .setLabel(value.getNumericValue() + "")
                                .build()
                )
        );

        var literals = languageLiterals
                .merge(appellationLiterals)
                .merge(langStringLiterals)
                .merge(placeLiterals)
                .merge(timePrimitiveLiterals)
                .merge(dimensionLiterals);

        var literalTable = literals.toTable(
                Materialized.<LiteralKey, LiteralValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.literals)
                        .withKeySerde(avroSerdes.LiteralKey())
                        .withValueSerde(avroSerdes.LiteralValue())
        );

        var statementEnrichedTable = infStatementTable.leftJoin(
                literalTable,
                value -> LiteralKey.newBuilder().setId(value.getFkObjectInfo()).build(),
                (statement, literal) -> StatementEnrichedValue.newBuilder()
                        .setSubjectId(statement.getFkSubjectInfo())
                        .setPropertyId(statement.getFkProperty())
                        .setObjectId(statement.getFkObjectInfo())
                        .setObjectLiteral(literal)
                        .setDeleted$1(Utils.stringIsNotEqualTrue(statement.getDeleted$1()))
                        .build(),
                Materialized.<dev.information.statement.Key, StatementEnrichedValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.statement_enriched)
                        .withKeySerde(avroSerdes.InfStatementKey())
                        .withValueSerde(avroSerdes.StatementEnrichedValue())
        );

        var statementsEnrichedStream = statementEnrichedTable
                .toStream()
                .map((key, value) -> KeyValue.pair(
                        StatementEnrichedKey.newBuilder()
                                .setSubjectId(value.getSubjectId())
                                .setPropertyId(value.getPropertyId())
                                .setObjectId(value.getObjectId())
                                .build(),
                        value
                ));

        statementsEnrichedStream.to(output.TOPICS.statement_enriched,
                Produced.with(avroSerdes.StatementEnrichedKey(), avroSerdes.StatementEnrichedValue()));

        return builder;

    }


    public enum input {
        TOPICS;
        public final String inf_statement = DbTopicNames.inf_statement.getName();
        public final String inf_language = DbTopicNames.inf_language.getName();
        public final String inf_appellation = DbTopicNames.inf_appellation.getName();
        public final String inf_lang_string = DbTopicNames.inf_lang_string.getName();
        public final String inf_place = DbTopicNames.inf_place.getName();
        public final String inf_time_primitive = DbTopicNames.inf_time_primitive.getName();
        public final String inf_dimension = DbTopicNames.inf_dimension.getName();
    }


    public enum inner {
        TOPICS;
        public final String literals = "literals";
        public final String statement_enriched = "statement_enriched";
    }

    public enum output {
        TOPICS;
        public final String statement_enriched = Utils.tsPrefixed("statement_enriched");
    }

}
