package org.geovistory.toolbox.streams.nodes.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.GeoUtils;
import org.geovistory.toolbox.streams.lib.TopicNameEnum;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.lib.jsonmodels.CommunityVisibility;
import org.geovistory.toolbox.streams.nodes.AvroSerdes;
import org.geovistory.toolbox.streams.nodes.BuilderSingleton;
import org.geovistory.toolbox.streams.nodes.RegisterInputTopic;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Objects;

@ApplicationScoped
public class Nodes {

    public static final int MAX_STRING_LENGTH = 100;
    private final ObjectMapper mapper = new ObjectMapper();

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

    public Nodes(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
    }

    public void addProcessorsStandalone() {

        addProcessors(
                registerInputTopic.infResourceStream(),
                registerInputTopic.infLanguageStream(),
                registerInputTopic.infAppellationStream(),
                registerInputTopic.infLangStringStream(),
                registerInputTopic.infPlaceStream(),
                registerInputTopic.infTimePrimitiveStream(),
                registerInputTopic.infDimensionStream(),
                registerInputTopic.datDigitalStream(),
                registerInputTopic.tabCellStream()
        );
    }

    public void addProcessors(
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

        // Map entities to nodes
        var entityNodes = infResourceTable
                .filter(
                        (key, value) -> value.getFkClass() != null,
                        Named.as("kstream-filter-inf-resource-with-class-id")
                )
                .map((key, value) -> KeyValue.pair(
                                NodeKey.newBuilder().setId("i" + value.getPkEntity()).build(),
                                NodeValue.newBuilder().setId("i" + value.getPkEntity())
                                        .setEntity(tranformEntity(value))
                                        .setClassId(value.getFkClass())
                                        .build()
                        ),
                        Named.as("kstream-map-inf-resource-to-entity-object")
                );

        // Map languages to nodes
        var languageNodes = infLanguageStream.map((key, value) -> KeyValue.pair(
                        NodeKey.newBuilder().setId("i" + value.getPkEntity()).build(),
                        NodeValue.newBuilder().setId("i" + value.getPkEntity())
                                .setLanguage(tranformLanguage(value))
                                .setClassId(value.getFkClass())
                                .setLabel(value.getNotes())
                                .build()
                ),
                Named.as("kstream-map-inf-language-to-language-object")
        );


        // Map appellations to nodes
        var appellationNodes = infAppellationStream.map((key, value) -> {
                    var transformedValue = tranformAppellation(value);
                    return KeyValue.pair(
                            NodeKey.newBuilder().setId("i" + key.getPkEntity()).build(),
                            NodeValue.newBuilder().setId("i" + key.getPkEntity())
                                    .setAppellation(transformedValue)
                                    .setClassId(value.getFkClass())
                                    .setLabel(transformedValue.getString())
                                    .build()
                    );
                },
                Named.as("kstream-map-inf-appellation-to-appellation-object")
        );

        // Map langStrings to nodes
        var langStringNodes = infLangStringStream.map((key, value) -> {
                    var transformedValue = tranformLangString(value);
                    return KeyValue.pair(
                            NodeKey.newBuilder().setId("i" + key.getPkEntity()).build(),
                            NodeValue.newBuilder().setId("i" + key.getPkEntity())
                                    .setLangString(transformedValue)
                                    .setClassId(value.getFkClass())
                                    .setLabel(transformedValue.getString())
                                    .build()
                    );
                },
                Named.as("kstream-map-inf-lang_string-to-lang_string-object")
        );

        // Map places to nodes
        var placeNodes = infPlaceStream.map((key, value) -> {

                    // if geoPoint is null (it can be considered as invalid)
                    if (value.getGeoPoint() == null) {
                        // create a default geo point to not break downstream applications
                        var fallback = io.debezium.data.geometry.Geography.newBuilder()
                                .setWkb(GeoUtils.pointToBytes(0, 0, 4326))
                                .setSrid(4326)
                                .build();
                        value.setGeoPoint(fallback);
                    }
                    var wkb = value.getGeoPoint().getWkb();
                    var point = GeoUtils.bytesToPoint(wkb);
                    var x = point.getX();
                    var y = point.getY();

                    return KeyValue.pair(
                            NodeKey.newBuilder().setId("i" + key.getPkEntity()).build(),
                            NodeValue.newBuilder().setId("i" + key.getPkEntity())
                                    .setPlace(tranformPlace(value))
                                    .setClassId(value.getFkClass())
                                    .setLabel(
                                            "WGS84: " + x + "°, " + y + "°"
                                    ).build()
                    );
                },
                Named.as("kstream-map-inf-place-to-place-object")
        );

        // Map timePrimitives to nodes
        var timePrimitiveNodes = infTimePrimitiveStream.map((key, value) -> KeyValue.pair(
                        NodeKey.newBuilder().setId("i" + key.getPkEntity()).build(),
                        NodeValue.newBuilder().setId("i" + key.getPkEntity())
                                .setTimePrimitive(tranformTimePrimitive(value))
                                .setClassId(value.getFkClass())
                                .setLabel(null)
                                .build()
                ),
                Named.as("kstream-map-inf-time-primitive-to-time-primitive-object")
        );

        // Map dimensions to nodes
        var dimensionNodes = infDimensionStream.map((key, value) -> KeyValue.pair(
                        NodeKey.newBuilder().setId("i" + key.getPkEntity()).build(),
                        NodeValue.newBuilder().setId("i" + key.getPkEntity())
                                .setDimension(tranformDimension(value))
                                .setClassId(value.getFkClass())
                                .setLabel(value.getNumericValue() + "")
                                .build()
                ),
                Named.as("kstream-map-inf-dimension-to-dimension-object")
        );

        // Map digital (table value) to nodes
        var tableValueNodes = datDigitalStream.map((key, value) -> KeyValue.pair(
                        NodeKey.newBuilder().setId("d" + key.getPkEntity()).build(),
                        NodeValue.newBuilder().setId("d" + key.getPkEntity())
                                .setDigital(tranformDigital(value))
                                .setClassId(936) // https://ontome.net/ontology/c936
                                .setLabel(null)
                                .build()
                ),
                Named.as("kstream-map-dat-digital-to-digital-object")
        );

        // Map cell to nodes
        var cellNodes = tabCellStream.map((key, value) -> KeyValue.pair(
                        NodeKey.newBuilder().setId("t" + key.getPkCell()).build(),
                        NodeValue.newBuilder().setId("t" + key.getPkCell())
                                .setCell(tranformCell(value))
                                .setClassId(521) // https://ontome.net/ontology/c521
                                .setLabel(null)
                                .build()
                ),
                Named.as("kstream-map-tab-cell-to-cell-object")
        );

        var nodes = entityNodes
                .merge(languageNodes, Named.as("kstream-merge-language-nodes"))
                .merge(appellationNodes, Named.as("kstream-merge-appellation-nodes"))
                .merge(langStringNodes, Named.as("kstream-merge-langString-nodes"))
                .merge(placeNodes, Named.as("kstream-merge-place-nodes"))
                .merge(timePrimitiveNodes, Named.as("kstream-merge-timePrimitive-nodes"))
                .merge(dimensionNodes, Named.as("kstream-merge-dimension-nodes"))
                .merge(tableValueNodes, Named.as("kstream-merge-tableValue-nodes"))
                .merge(cellNodes, Named.as("kstream-merge-cell-nodes"))

                // repartition
                .repartition(
                        Repartitioned.<NodeKey, NodeValue>as("nodes-repartitioned")
                                .withKeySerde(avroSerdes.NodeKey())
                                .withValueSerde(avroSerdes.NodeValue())
                );


        nodes.to(
                outNodes(),
                Produced.with(avroSerdes.NodeKey(), avroSerdes.NodeValue())
                        .withName(outNodes() + "-producer")
        );

        // if "true" the app creates a topic "nodes_flat" that can be sinked to postgres
        if (Objects.equals(createOutputForPostgres, "true")) {
            builderSingleton.builder.stream(
                            outNodes(),
                            Consumed.with(avroSerdes.NodeKey(), avroSerdes.NodeValue())
                                    .withName(outNodes() + "-consumer")
                    )
                    .mapValues((readOnlyKey, value) -> TextValue.newBuilder().setText(value.toString()).build())
                    .to(
                            outNodesFlat(),
                            Produced
                                    .with(avroSerdes.NodeKey(), avroSerdes.TextValue())
                                    .withName(outNodesFlat() + "-producer")
                    );

        }

    }


    /**
     * @param infEntity the value from the database
     * @return a projected, more lightweight, value
     */
    private Entity tranformEntity(dev.information.resource.Value infEntity) {
        var communityCanSeeInToolbox = false;
        var communityCanSeeInDataApi = false;
        var communityCanSeeInWebsite = false;
        if (infEntity.getCommunityVisibility() != null) {

            try {
                var communitVisibility = mapper.readValue(infEntity.getCommunityVisibility(), CommunityVisibility.class);
                if (communitVisibility.toolbox) communityCanSeeInToolbox = true;
                if (communitVisibility.dataApi) communityCanSeeInDataApi = true;
                if (communitVisibility.website) communityCanSeeInWebsite = true;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return Entity.newBuilder()
                .setPkEntity(infEntity.getPkEntity())
                .setFkClass(infEntity.getFkClass())
                .setCommunityVisibilityToolbox(communityCanSeeInToolbox)
                .setCommunityVisibilityDataApi(communityCanSeeInDataApi)
                .setCommunityVisibilityWebsite(communityCanSeeInWebsite)
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
                .setFkClass(tabCell.getFkClass())
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
                .setSetIso6391(infLanguage.getIso6391())
                .setSetIso6392b(infLanguage.getIso6392b())
                .setSetIso6392t(infLanguage.getIso6392t())
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


    public String inInfResource() {
        return Utils.prefixedIn(inPrefix, TopicNameEnum.inf_resource.getValue());
    }

    public String inInfLanguage() {
        return Utils.prefixedIn(inPrefix, TopicNameEnum.inf_language.getValue());
    }

    public String inInfAppellation() {
        return Utils.prefixedIn(inPrefix, TopicNameEnum.inf_appellation.getValue());
    }

    public String inInfLangString() {
        return Utils.prefixedIn(inPrefix, TopicNameEnum.inf_lang_string.getValue());
    }

    public String inInfPlace() {
        return Utils.prefixedIn(inPrefix, TopicNameEnum.inf_place.getValue());
    }

    public String inInfTimePrimitive() {
        return Utils.prefixedIn(inPrefix, TopicNameEnum.inf_time_primitive.getValue());
    }

    public String inInfDimension() {
        return Utils.prefixedIn(inPrefix, TopicNameEnum.inf_dimension.getValue());
    }

    public String inDatDigital() {
        return Utils.prefixedIn(inPrefix, TopicNameEnum.dat_digital.getValue());
    }

    public String inTabCell() {
        return Utils.prefixedIn(inPrefix, TopicNameEnum.tab_cell.getValue());
    }


    public String outNodes() {
        return Utils.prefixedOut(outPrefix, "nodes");
    }

    public String outNodesFlat() {
        return Utils.prefixedOut(outPrefix, "nodes_flat");
    }


}
