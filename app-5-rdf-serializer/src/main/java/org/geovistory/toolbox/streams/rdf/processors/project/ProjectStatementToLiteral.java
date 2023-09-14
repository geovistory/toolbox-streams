package org.geovistory.toolbox.streams.rdf.processors.project;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.GeoUtils;
import org.geovistory.toolbox.streams.lib.TimeUtils;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.rdf.AvroSerdes;
import org.geovistory.toolbox.streams.rdf.OutputTopicNames;
import org.geovistory.toolbox.streams.rdf.RegisterInputTopic;
import org.geovistory.toolbox.streams.utilities.StringSanitizer;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.geovistory.toolbox.streams.lib.CommonUris.*;
import static org.geovistory.toolbox.streams.lib.UrlPrefixes.*;
import static org.geovistory.toolbox.streams.lib.Utils.getLanguageFromId;


@ApplicationScoped
public class ProjectStatementToLiteral {


    @Inject
    AvroSerdes avroSerdes;

    @Inject
    RegisterInputTopic registerInputTopic;


    @Inject
    OutputTopicNames outputTopicNames;

    public ProjectStatementToLiteral(AvroSerdes avroSerdes, RegisterInputTopic registerInputTopic, OutputTopicNames outputTopicNames) {
        this.avroSerdes = avroSerdes;
        this.registerInputTopic = registerInputTopic;
        this.outputTopicNames = outputTopicNames;
    }

    public void addProcessorsStandalone() {
        addProcessors(
                registerInputTopic.projectStatementWithLiteralStream()
        );
    }

    public ProjectRdfReturnValue addProcessors(
            KStream<ProjectStatementKey, ProjectStatementValue> projectStatementWithEntityStream
    ) {

        /* STREAM PROCESSORS */
        // 2)

        var s = projectStatementWithEntityStream.flatMap(
                (key, value) -> {
                    List<KeyValue<ProjectRdfKey, ProjectRdfValue>> result = new LinkedList<>();

                    //value of operation
                    var v = ProjectRdfValue.newBuilder()
                            .setOperation(
                                    Utils.booleanIsEqualTrue(value.getDeleted$1()) ? Operation.delete : Operation.insert)
                            .build();

                    //get subject, object and property ids
                    var subjectId = value.getStatement().getSubjectId();
                    var propertyId = value.getStatement().getPropertyId();
                    var objectId = value.getStatement().getObjectId();
                    var language = value.getStatement().getObject().getLanguage();
                    var appellation = value.getStatement().getObject().getAppellation();
                    var langString = value.getStatement().getObject().getLangString();
                    var place = value.getStatement().getObject().getPlace();
                    var timePrimitive = value.getStatement().getObject().getTimePrimitive();
                    var dimension = value.getStatement().getObject().getDimension();
                    //var cell = value.getStatement().getObject().getCell();
                    //var digital = value.getStatement().getObject().getDigital();
                    ArrayList<String> turtles = new ArrayList<>();
                    var julianUri = JULIAN.getUri();
                    var gregorianUri = GREGORIAN.getUri();
                    var calendarSystemUri = "";

                    // add the language triple
                    if (language != null) {
                        var lng = "";
                        if (language.getNotes() != null) {
                            lng = language.getNotes();
                        } else lng = language.getPkLanguage();
                        //example: <http://geovistory.org/resource/i1761647> <https://ontome.net/ontology/p1112> "Italian"^^<http://www.w3.org/2001/XMLSchema#string> .
                        turtles.add("<" + GEOVISTORY_RESOURCE.getUrl() + subjectId + "> <" + ONTOME_PROPERTY.getUrl() + propertyId + "> \"" + StringSanitizer.escapeBackslashAndDoubleQuote(lng) + "\"^^<" + XSD_STRING.getUri() + "> .");
                    } else if (appellation != null) {
                        //example: <http://geovistory.org/resource/i1761647> <https://ontome.net/ontology/p1113> "Foo"^^<http://www.w3.org/2001/XMLSchema#string> .
                        turtles.add("<" + GEOVISTORY_RESOURCE.getUrl() + subjectId + "> <" + ONTOME_PROPERTY.getUrl() + propertyId + "> \"" + StringSanitizer.escapeBackslashAndDoubleQuote(appellation.getString()) + "\"^^<" + XSD_STRING.getUri() + "> .");
                    } else if (langString != null) {
                        var lng = "";
                        lng = getLanguageFromId(langString.getFkLanguage());
                        if (lng == null) lng = "" + langString.getFkLanguage();
                        //example: <http://geovistory.org/resource/i1761647> <https://ontome.net/ontology/p1113> "Bar"@it .
                        turtles.add("<" + GEOVISTORY_RESOURCE.getUrl() + subjectId + "> <" + ONTOME_PROPERTY.getUrl() + propertyId + "> \"" + langString.getString() + "\"@" + lng + " .");
                    } else if (place != null) {
                        var wkb = place.getGeoPoint().getWkb();
                        var point = GeoUtils.bytesToPoint(wkb);
                        //example: <http://geovistory.org/resource/i1761647> <https://ontome.net/ontology/p1113> "<http://www.opengis.net/def/crs/EPSG/0/4326>POINT(2.348611 48.853333)"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .
                        turtles.add("<" + GEOVISTORY_RESOURCE.getUrl() + subjectId + "> <" + ONTOME_PROPERTY.getUrl() + propertyId + "> \"<" + EPSG_4326.getUri() + ">POINT(" + point.getX() + " " + point.getY() + ")\"^^<" + OPENGIS_WKT.getUri() + "> .");
                    } else if (timePrimitive != null) {
                        var durationUnit = timePrimitive.getDuration();
                        var julianDay = timePrimitive.getJulianDay();
                        if (julianDay != null && durationUnit != null) {
                            var julianYMD = TimeUtils.getYearMonthDay(julianDay, TimeUtils.CalendarType.julian).toString();
                            var gregorianYMD = TimeUtils.getYearMonthDay(julianDay, TimeUtils.CalendarType.gregorian).toString();
                            var ymd = "";
                            var y = "";
                            var m = "";
                            var d = "";

                            var label = "";

                            if (timePrimitive.getCalendar().equals("julian")) {
                                calendarSystemUri = julianUri;
                                ymd = julianYMD;
                            } else {
                                calendarSystemUri = gregorianUri;
                                ymd = gregorianYMD;
                            }

                            y = ymd.substring(0, 4);
                            m = ymd.substring(5, 7);
                            d = ymd.substring(8, 10);

                            switch (durationUnit) {
                                case "1 year" -> {
                                    durationUnit = "unitYear";
                                    label = y;
                                }
                                case "1 month" -> {
                                    durationUnit = "unitMonth";
                                    label = y + "-" + m;
                                }
                                case "1 day" -> {
                                    durationUnit = "unitDay";
                                    label = y + "-" + m + "-" + d;
                                }
                            }

                            turtles.add("<" + GEOVISTORY_RESOURCE.getUrl() + subjectId + "ts> <" + ONTOME_PROPERTY.getUrl() + propertyId + "> <" + GEOVISTORY_RESOURCE.getUrl() + objectId + ">");
                            turtles.add("<" + GEOVISTORY_RESOURCE.getUrl() + objectId + "> <" + ONTOME_PROPERTY.getUrl() + propertyId + "i> <" + GEOVISTORY_RESOURCE.getUrl() + subjectId + "ts>");
                            turtles.add("<" + GEOVISTORY_RESOURCE.getUrl() + objectId + "> a <" + TIME.getUrl() + "DateTimeDescription>");
                            turtles.add("<" + GEOVISTORY_RESOURCE.getUrl() + objectId + "> <" + TIME.getUrl() + "hasTRS> <" + calendarSystemUri + ">");
                            turtles.add("<" + GEOVISTORY_RESOURCE.getUrl() + objectId + "> <" + TIME.getUrl() + "day> \"---" + d + "\"^^<" + TIME.getUrl() + "generalDay>");
                            turtles.add("<" + GEOVISTORY_RESOURCE.getUrl() + objectId + "> <" + TIME.getUrl() + "month> \"--" + m + "\"^^<" + TIME.getUrl() + "generalMonth>");
                            turtles.add("<" + GEOVISTORY_RESOURCE.getUrl() + objectId + "> <" + TIME.getUrl() + "year> \"-" + y + "\"^^<" + TIME.getUrl() + "generalYear>");
                            turtles.add("<" + GEOVISTORY_RESOURCE.getUrl() + objectId + "> <" + TIME.getUrl() + "hasTRS> <" + TIME.getUrl() + durationUnit + ">");
                            turtles.add("<" + GEOVISTORY_RESOURCE.getUrl() + objectId + "> <" + TIME.getUrl() + "year> \"" + StringSanitizer.escapeBackslashAndDoubleQuote(label) + "\"^^<" + XSD_STRING.getUri() + ">");
                        }
                    } else if (dimension != null) {
                        DecimalFormat format = new DecimalFormat("0.#");
                        var numericValue = format.format(dimension.getNumericValue()); //removes the trailing "0"
                        var fkMeasurementUnit = dimension.getFkMeasurementUnit();

                        turtles.add("<" + GEOVISTORY_RESOURCE.getUrl() + subjectId + "> <" + ONTOME_PROPERTY.getUrl() + propertyId + "> <" + GEOVISTORY_RESOURCE.getUrl() + objectId + ">");
                        turtles.add("<" + GEOVISTORY_RESOURCE.getUrl() + objectId + "> <" + ONTOME_PROPERTY.getUrl() + propertyId + "i> <" + GEOVISTORY_RESOURCE.getUrl() + subjectId + ">");
                        turtles.add("<" + GEOVISTORY_RESOURCE.getUrl() + objectId + "> <" + ONTOME_PROPERTY.getUrl() + "78> \"" + numericValue + "\"^^<" + XSD.getUrl() + "decimal>");
                        turtles.add("<" + GEOVISTORY_RESOURCE.getUrl() + objectId + "> <" + ONTOME_PROPERTY.getUrl() + "79> <" + GEOVISTORY_RESOURCE.getUrl() + "i" + fkMeasurementUnit + ">");
                        turtles.add("<" + GEOVISTORY_RESOURCE.getUrl() + "i" + fkMeasurementUnit + "> <" + ONTOME_PROPERTY.getUrl() + "79i> <" + GEOVISTORY_RESOURCE.getUrl() + objectId + ">");

                    }

                    ProjectRdfKey k;
                    for (String item : turtles) {
                        k = ProjectRdfKey.newBuilder()
                                .setProjectId(value.getProjectId())
                                .setTurtle(item)
                                .build();
                        result.add(KeyValue.pair(k, v));
                    }
                    // create a stream of key-value pairs

                    return result;
                }
        );

        /* SINK PROCESSORS */
        s.to(outputTopicNames.projectRdf(),
                Produced.with(avroSerdes.ProjectRdfKey(), avroSerdes.ProjectRdfValue())
                        .withName(outputTopicNames.projectRdf() + "-literal-producer")
        );

        return new ProjectRdfReturnValue(s);

    }

}
