package org.geovistory.toolbox.streams.topologies;

import dev.information.statement.Value;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.app.DbTopicNames;
import org.geovistory.toolbox.streams.app.RegisterInputTopic;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.GeoUtils;
import org.geovistory.toolbox.streams.lib.Utils;

import java.util.Map;


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
                registerInputTopic.infResourceStream(),
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
            KStream<dev.information.resource.Key, dev.information.resource.Value> infResourceTable,
            KStream<dev.information.language.Key, dev.information.language.Value> infLanguageStream,
            KStream<dev.information.appellation.Key, dev.information.appellation.Value> infAppellationStream,
            KStream<dev.information.lang_string.Key, dev.information.lang_string.Value> infLangStringStream,
            KStream<dev.information.place.Key, dev.information.place.Value> infPlaceStream,
            KStream<dev.information.time_primitive.Key, dev.information.time_primitive.Value> infTimePrimitiveStream,
            KStream<dev.information.dimension.Key, dev.information.dimension.Value> infDimensionStream,
            KStream<dev.data.digital.Key, dev.data.digital.Value> datDigitalStream,
            KStream<dev.tables.cell.Key, dev.tables.cell.Value> tabCellStream


    ) {

        var avroSerdes = new ConfluentAvroSerdes();

        // Map entities to objects
        var entityObjects = infResourceTable
                .filter((key, value) -> value.getFkClass() != null)
                .map((key, value) -> KeyValue.pair(
                        ObjectKey.newBuilder().setId("i" + value.getPkEntity()).build(),
                        ObjectValue.newBuilder().setId("i" + value.getPkEntity())
                                .setEntity(tranformEntity(value))
                                .setClassId(value.getFkClass())
                                .build()
                ));

        // Map languages to objects
        var languageObjects = infLanguageStream.map((key, value) -> KeyValue.pair(
                ObjectKey.newBuilder().setId("i" + value.getPkEntity()).build(),
                ObjectValue.newBuilder().setId("i" + value.getPkEntity())
                        .setLanguage(tranformLanguage(value))
                        .setClassId(value.getFkClass())
                        .setLabel(value.getNotes())
                        .build()
        ));


        // Map appellations to objects
        var appellationObjects = infAppellationStream.map((key, value) -> {
            var transformedValue = tranformAppellation(value);
            return KeyValue.pair(
                    ObjectKey.newBuilder().setId("i" + key.getPkEntity()).build(),
                    ObjectValue.newBuilder().setId("i" + key.getPkEntity())
                            .setAppellation(transformedValue)
                            .setClassId(value.getFkClass())
                            .setLabel(transformedValue.getString())
                            .build()
            );
        });

        // Map langStrings to objects
        var langStringObjects = infLangStringStream.map((key, value) -> {
            var transformedValue = tranformLangString(value);
            return KeyValue.pair(
                    ObjectKey.newBuilder().setId("i" + key.getPkEntity()).build(),
                    ObjectValue.newBuilder().setId("i" + key.getPkEntity())
                            .setLangString(transformedValue)
                            .setClassId(value.getFkClass())
                            .setLabel(transformedValue.getString())
                            .build()
            );
        });

        // Map places to objects
        var placeObjects = infPlaceStream.map((key, value) -> {
                    var wkb = value.getGeoPoint().getWkb();
                    var point = GeoUtils.bytesToPoint(wkb);
                    var x = point.getX();
                    var y = point.getY();
                    return KeyValue.pair(
                            ObjectKey.newBuilder().setId("i" + key.getPkEntity()).build(),
                            ObjectValue.newBuilder().setId("i" + key.getPkEntity())
                                    .setPlace(tranformPlace(value))
                                    .setClassId(value.getFkClass())
                                    .setLabel(
                                            "WGS84: " + x + "°, " + y + "°"
                                    ).build()
                    );
                }
        );

        // Map timePrimitives to objects
        var timePrimitiveObjects = infTimePrimitiveStream.map((key, value) -> KeyValue.pair(
                        ObjectKey.newBuilder().setId("i" + key.getPkEntity()).build(),
                        ObjectValue.newBuilder().setId("i" + key.getPkEntity())
                                .setTimePrimitive(tranformTimePrimitive(value))
                                .setClassId(value.getFkClass())
                                .setLabel(null)
                                .build()
                )
        );

        // Map dimensions to objects
        var dimensionObjects = infDimensionStream.map((key, value) -> KeyValue.pair(
                        ObjectKey.newBuilder().setId("i" + key.getPkEntity()).build(),
                        ObjectValue.newBuilder().setId("i" + key.getPkEntity())
                                .setDimension(tranformDimension(value))
                                .setClassId(value.getFkClass())
                                .setLabel(value.getNumericValue() + "")
                                .build()
                )
        );

        // Map digital (table value) to objects
        var tableValueLiteral = datDigitalStream.map((key, value) -> KeyValue.pair(
                        ObjectKey.newBuilder().setId("d" + key.getPkEntity()).build(),
                        ObjectValue.newBuilder().setId("d" + key.getPkEntity())
                                .setDigital(tranformDigital(value))
                                .setClassId(936) // https://ontome.net/ontology/c936
                                .setLabel(null)
                                .build()
                )
        );

        // Map cell to objects
        var cellLiteral = tabCellStream.map((key, value) -> KeyValue.pair(
                        ObjectKey.newBuilder().setId("t" + key.getPkCell()).build(),
                        ObjectValue.newBuilder().setId("t" + key.getPkCell())
                                .setCell(tranformCell(value))
                                .setClassId(521) // https://ontome.net/ontology/c521
                                .setLabel(null)
                                .build()
                )
        );

        var objects = entityObjects
                .merge(languageObjects)
                .merge(appellationObjects)
                .merge(langStringObjects)
                .merge(placeObjects)
                .merge(timePrimitiveObjects)
                .merge(dimensionObjects)
                .merge(tableValueLiteral)
                .merge(cellLiteral);

        var literalTable = objects.toTable(
                Materialized.<ObjectKey, ObjectValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.literals)
                        .withKeySerde(avroSerdes.LiteralKey())
                        .withValueSerde(avroSerdes.LiteralValue())
        );

        var statementJoinedWithObjectTable = infStatementTable.join(
                literalTable,
                value -> ObjectKey.newBuilder()
                        .setId(getObjectStringId(value))
                        .build(),
                (statement, object) -> {
                    if (object == null) return null;
                    return StatementEnrichedValue.newBuilder()
                            .setSubjectId(getSubjectStringId(statement))
                            .setPropertyId(statement.getFkProperty())
                            .setObjectId(getObjectStringId(statement))
                            .setObjectLabel(object.getLabel())
                            .setObject(object)
                            .setObjectClassId(object.getClassId())
                            .setDeleted$1(Utils.stringIsEqualTrue(statement.getDeleted$1()))
                            .build();
                },
                Materialized.<dev.information.statement.Key, StatementEnrichedValue, KeyValueStore<Bytes, byte[]>>as(inner.TOPICS.statement_joined_with_object)
                        .withKeySerde(avroSerdes.InfStatementKey())
                        .withValueSerde(avroSerdes.StatementEnrichedValue())
        );

        var stream = statementJoinedWithObjectTable.toStream();
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

        e.to(output.TOPICS.statement_with_entity, Produced.with(avroSerdes.InfStatementKey(), avroSerdes.StatementEnrichedValue()));
        l.to(output.TOPICS.statement_with_literal, Produced.with(avroSerdes.InfStatementKey(), avroSerdes.StatementEnrichedValue()));
        o.to(output.TOPICS.statement_other, Produced.with(avroSerdes.InfStatementKey(), avroSerdes.StatementEnrichedValue()));

        return new StatementEnrichedReturnValue(builder, stream);

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
     * @param infEntity the value from the database
     * @return a projected, more lightweight, value
     */
    private static Entity tranformEntity(dev.information.resource.Value infEntity) {
        return Entity.newBuilder()
                .setPkEntity(infEntity.getPkEntity())
                .setFkClass(infEntity.getFkClass())
                .build();
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
        public final String inf_resource = DbTopicNames.inf_resource.getName();
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
        public final String statement_joined_with_object = "statement_joined_with_object";
    }

    public enum output {
        TOPICS;
        public final String statement_with_entity = Utils.tsPrefixed("statement_with_entity");
        public final String statement_with_literal = Utils.tsPrefixed("statement_with_literal");
        public final String statement_other = Utils.tsPrefixed("statement_other");
    }

}
