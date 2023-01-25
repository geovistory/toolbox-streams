package org.geovistory.toolbox.streams.topologies;

import dev.information.statement.Value;
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
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.GeoUtils;
import org.geovistory.toolbox.streams.lib.Utils;


public class StatementEnriched {

    public static final int MAX_STRING_LENGTH = 100;

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
                registerInputTopic.infDimensionStream(),
                registerInputTopic.datDigitalStream(),
                registerInputTopic.tabCellStream()
        ).builder().build();
    }

    public static StatementEnrichedReturnValue addProcessors(
            StreamsBuilder builder,
            KTable<dev.information.statement.Key, dev.information.statement.Value> infStatementTable,
            KStream<dev.information.language.Key, dev.information.language.Value> infLanguageTable,
            KStream<dev.information.appellation.Key, dev.information.appellation.Value> infAppellationTable,
            KStream<dev.information.lang_string.Key, dev.information.lang_string.Value> infLangStringTable,
            KStream<dev.information.place.Key, dev.information.place.Value> infPlaceTable,
            KStream<dev.information.time_primitive.Key, dev.information.time_primitive.Value> infTimePrimitiveTable,
            KStream<dev.information.dimension.Key, dev.information.dimension.Value> infDimensionTable,
            KStream<dev.data.digital.Key, dev.data.digital.Value> datDigitalTable,
            KStream<dev.tables.cell.Key, dev.tables.cell.Value> tabCellTable

    ) {

        var avroSerdes = new ConfluentAvroSerdes();

        // Map languages to literals
        var languageLiterals = infLanguageTable.map((key, value) -> KeyValue.pair(
                LiteralKey.newBuilder().setId("i" + value.getPkEntity()).build(),
                LiteralValue.newBuilder().setId("i" + value.getPkEntity())
                        .setLanguage(tranformLanguage(value))
                        .setClassId(value.getFkClass())
                        .setLabel(value.getNotes())
                        .build()
        ));


        // Map appellations to literals
        var appellationLiterals = infAppellationTable.map((key, value) -> {
            var transformedValue = tranformAppellation(value);
            return KeyValue.pair(
                    LiteralKey.newBuilder().setId("i" + key.getPkEntity()).build(),
                    LiteralValue.newBuilder().setId("i" + key.getPkEntity())
                            .setAppellation(transformedValue)
                            .setClassId(value.getFkClass())
                            .setLabel(transformedValue.getString())
                            .build()
            );
        });

        // Map langStrings to literals
        var langStringLiterals = infLangStringTable.map((key, value) -> {
            var transformedValue = tranformLangString(value);
            return KeyValue.pair(
                    LiteralKey.newBuilder().setId("i" + key.getPkEntity()).build(),
                    LiteralValue.newBuilder().setId("i" + key.getPkEntity())
                            .setLangString(transformedValue)
                            .setClassId(value.getFkClass())
                            .setLabel(transformedValue.getString())
                            .build()
            );
        });

        // Map places to literals
        var placeLiterals = infPlaceTable.map((key, value) -> {
                    var wkb = value.getGeoPoint().getWkb();
                    var point = GeoUtils.bytesToPoint(wkb);
                    var x = point.getX();
                    var y = point.getY();
                    return KeyValue.pair(
                            LiteralKey.newBuilder().setId("i" + key.getPkEntity()).build(),
                            LiteralValue.newBuilder().setId("i" + key.getPkEntity())
                                    .setPlace(tranformPlace(value))
                                    .setClassId(value.getFkClass())
                                    .setLabel(
                                            "WGS84: " + x + "°, " + y + "°"
                                    ).build()
                    );
                }
        );

        // Map timePrimitives to literals
        var timePrimitiveLiterals = infTimePrimitiveTable.map((key, value) -> KeyValue.pair(
                        LiteralKey.newBuilder().setId("i" + key.getPkEntity()).build(),
                        LiteralValue.newBuilder().setId("i" + key.getPkEntity())
                                .setTimePrimitive(tranformTimePrimitive(value))
                                .setClassId(value.getFkClass())
                                .setLabel(null)
                                .build()
                )
        );

        // Map dimensions to literals
        var dimensionLiterals = infDimensionTable.map((key, value) -> KeyValue.pair(
                        LiteralKey.newBuilder().setId("i" + key.getPkEntity()).build(),
                        LiteralValue.newBuilder().setId("i" + key.getPkEntity())
                                .setDimension(tranformDimension(value))
                                .setClassId(value.getFkClass())
                                .setLabel(value.getNumericValue() + "")
                                .build()
                )
        );

        // Map digital (table value) to literals
        var tableValueLiteral = datDigitalTable.map((key, value) -> KeyValue.pair(
                        LiteralKey.newBuilder().setId("d" + key.getPkEntity()).build(),
                        LiteralValue.newBuilder().setId("d" + key.getPkEntity())
                                .setDigital(tranformDigital(value))
                                .setClassId(936) // https://ontome.net/ontology/c936
                                .setLabel(null)
                                .build()
                )
        );

        // Map cell to literals
        var cellLiteral = tabCellTable.map((key, value) -> KeyValue.pair(
                        LiteralKey.newBuilder().setId("t" + key.getPkCell()).build(),
                        LiteralValue.newBuilder().setId("t" + key.getPkCell())
                                .setCell(tranformCell(value))
                                .setClassId(521) // https://ontome.net/ontology/c521
                                .setLabel(null)
                                .build()
                )
        );

        var literals = languageLiterals
                .merge(appellationLiterals)
                .merge(langStringLiterals)
                .merge(placeLiterals)
                .merge(timePrimitiveLiterals)
                .merge(dimensionLiterals)
                .merge(tableValueLiteral)
                .merge(cellLiteral);

        var literalTable = literals.toTable(
                Materialized.<LiteralKey, LiteralValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.literals)
                        .withKeySerde(avroSerdes.LiteralKey())
                        .withValueSerde(avroSerdes.LiteralValue())
        );

        var statementEnrichedTable = infStatementTable.leftJoin(
                literalTable,
                value -> LiteralKey.newBuilder()
                        .setId(getObjectStringId(value))
                        .build(),
                (statement, literal) -> StatementEnrichedValue.newBuilder()
                        .setSubjectId(getSubjectStringId(statement))
                        .setPropertyId(statement.getFkProperty())
                        .setObjectId(getObjectStringId(statement))
                        .setObjectLabel(literal == null ? null : literal.getLabel())
                        .setObjectLiteral(literal)
                        .setDeleted$1(Utils.stringIsNotEqualTrue(statement.getDeleted$1()))
                        .build(),
                Materialized.<dev.information.statement.Key, StatementEnrichedValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.statement_enriched)
                        .withKeySerde(avroSerdes.InfStatementKey())
                        .withValueSerde(avroSerdes.StatementEnrichedValue())
        );

        var statementsEnrichedStream = statementEnrichedTable
                .toStream();


        statementsEnrichedStream.to(output.TOPICS.statement_enriched,
                Produced.with(avroSerdes.InfStatementKey(), avroSerdes.StatementEnrichedValue()));

        return new StatementEnrichedReturnValue(builder, statementEnrichedTable, statementsEnrichedStream);

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

    /**
     * @param infAppellation the value from the database
     * @return a projected, more lightweight, value
     */
    private static Appellation tranformAppellation(dev.information.appellation.Value infAppellation) {
        return Appellation.newBuilder()
                .setPkEntity(infAppellation.getPkEntity())
                .setFkClass(infAppellation.getFkClass())
                .setString(Utils.shorten(infAppellation.getString(), MAX_STRING_LENGTH))
                .build();
    }


    /**
     * @param tabCell the value from the database
     * @return a projected, more lightweight, value
     */
    private static Cell tranformCell(dev.tables.cell.Value tabCell) {
        return Cell.newBuilder()
                .setPkCell(tabCell.getPkCell())
                .setFkColumn(tabCell.getFkColumn())
                .setFkRow(tabCell.getFkColumn())
                .setFkDigital(tabCell.getFkDigital())
                .setNumericValue(tabCell.getNumericValue())
                .setStringValue(tabCell.getStringValue())
                .build();
    }


    /**
     * @param datDigital the value from the database
     * @return a projected, more lightweight, value
     */
    private static Digital tranformDigital(dev.data.digital.Value datDigital) {
        return Digital.newBuilder()
                .setPkEntity(datDigital.getPkEntity())
                .setFkNamespace(datDigital.getFkNamespace())
                .setFkSystemType(datDigital.getFkSystemType())
                .build();
    }

    /**
     * @param infDimension the value from the database
     * @return a projected, more lightweight, value
     */
    private static Dimension tranformDimension(dev.information.dimension.Value infDimension) {
        return Dimension.newBuilder()
                .setPkEntity(infDimension.getPkEntity())
                .setFkClass(infDimension.getFkClass())
                .setNumericValue(infDimension.getNumericValue())
                .setFkMeasurementUnit(infDimension.getFkMeasurementUnit())
                .build();
    }


    /**
     * @param infLangString the value from the database
     * @return a projected, more lightweight, value
     */
    private static LangString tranformLangString(dev.information.lang_string.Value infLangString) {
        return LangString.newBuilder()
                .setPkEntity(infLangString.getPkEntity())
                .setFkClass(infLangString.getFkClass())
                .setString(Utils.shorten(infLangString.getString(), MAX_STRING_LENGTH))
                .setFkLanguage(infLangString.getFkLanguage())
                .build();
    }

    /**
     * @param infLanguage the value from the database
     * @return a projected, more lightweight, value
     */
    private static Language tranformLanguage(dev.information.language.Value infLanguage) {
        return Language.newBuilder()
                .setPkEntity(infLanguage.getPkEntity())
                .setFkClass(infLanguage.getFkClass())
                .setNotes(infLanguage.getNotes())
                .setPkLanguage(infLanguage.getPkLanguage())
                .build();
    }


    /**
     * @param infPlace the value from the database
     * @return a projected, more lightweight, value
     */
    private static Place tranformPlace(dev.information.place.Value infPlace) {
        return Place.newBuilder()
                .setPkEntity(infPlace.getPkEntity())
                .setFkClass(infPlace.getFkClass())
                .setGeoPoint(infPlace.getGeoPoint())
                .build();
    }

    /**
     * @param infTimePrimitive the value from the database
     * @return a projected, more lightweight, value
     */
    private static TimePrimitive tranformTimePrimitive(dev.information.time_primitive.Value infTimePrimitive) {
        return TimePrimitive.newBuilder()
                .setPkEntity(infTimePrimitive.getPkEntity())
                .setFkClass(infTimePrimitive.getFkClass())
                .setJulianDay(infTimePrimitive.getJulianDay())
                .setDuration(infTimePrimitive.getDuration())
                .setCalendar(infTimePrimitive.getCalendar())
                .build();
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
        public final String dat_digital = DbTopicNames.dat_digital.getName();
        public final String tab_cell = DbTopicNames.tab_cell.getName();
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
