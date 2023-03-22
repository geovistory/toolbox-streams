package org.geovistory.toolbox.streams.rdf.processors.project;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.ConfluentAvroSerdes;
import org.geovistory.toolbox.streams.lib.GeoUtils;
import org.geovistory.toolbox.streams.lib.TimeUtils;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.rdf.RegisterInputTopic;

import java.text.DecimalFormat;
import java.util.*;

import static org.geovistory.toolbox.streams.lib.UrlPrefixes.*;
import static org.geovistory.toolbox.streams.lib.CommonUris.*;
import static org.geovistory.toolbox.streams.lib.Utils.getLanguageFromId;


public class ProjectStatementToLiteral {

    public static void main(String[] args) {
        System.out.println(buildStandalone(new StreamsBuilder()).describe());
    }

    /**
     * Only used for the unit tests
     */
    public static Topology buildStandalone(StreamsBuilder builder) {
        var inputTopic = new RegisterInputTopic(builder);

        return addProcessors(
                builder,
                inputTopic.projectStatementWithLiteralStream()
        ).builder().build();
    }

    public static ProjectRdfReturnValue addProcessors(
            StreamsBuilder builder,
            KStream<ProjectStatementKey, ProjectStatementValue> projectStatementWithEntityStream
    ) {

        var avroSerdes = new ConfluentAvroSerdes();

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
                    var cell = value.getStatement().getObject().getCell();
                    var digital = value.getStatement().getObject().getDigital();
                    var turtle = "";
                    ArrayList<String> timePrimitiveTurtle = new ArrayList<>();
                    ArrayList<String> dimensionTurtle = new ArrayList<>();
                    var julianUri = JULIAN.getUri();
                    var gregorianUri = GREGORIAN.getUri();
                    var calendarSystemUri = "";

                    // add the language triple
                    if (language != null) {
                        var lng = "";
                        if(language.getNotes() != null) {
                            lng = language.getNotes();
                        }
                        else lng = language.getPkLanguage();
                        //example: <http://geovistory.org/resource/i1761647> <https://ontome.net/ontology/p1112> "Italian"^^<http://www.w3.org/2001/XMLSchema#string> .
                        turtle = "<"+ GEOVISTORY_RESOURCE.getUrl() +subjectId+"> <"+ ONTOME_PROPERTY.getUrl() +propertyId+"> \""+lng+"\"^^<"+ XSD_STRING.getUri() +"> .";
                    }
                    else if (appellation != null) {
                        //example: <http://geovistory.org/resource/i1761647> <https://ontome.net/ontology/p1113> "Foo"^^<http://www.w3.org/2001/XMLSchema#string> .
                        turtle = "<"+ GEOVISTORY_RESOURCE.getUrl() +subjectId+"> <"+ ONTOME_PROPERTY.getUrl() +propertyId+"> \""+appellation.getString()+"\"^^<"+ XSD_STRING.getUri() +"> .";
                    }
                    else if (langString != null) {
                        var lng = "";
                        lng = getLanguageFromId(langString.getFkLanguage());
                        if (lng == null) lng = ""+langString.getFkLanguage();
                        //example: <http://geovistory.org/resource/i1761647> <https://ontome.net/ontology/p1113> "Bar"@it .
                        turtle = "<"+GEOVISTORY_RESOURCE.getUrl()+subjectId+"> <"+ ONTOME_PROPERTY.getUrl() +propertyId+"> \""+langString.getString()+"\"@"+lng+" .";
                    }
                    else if (place != null) {
                        var wkb = place.getGeoPoint().getWkb();
                        var point = GeoUtils.bytesToPoint(java.util.Base64.getDecoder().decode(wkb));

                        //example: <http://geovistory.org/resource/i1761647> <https://ontome.net/ontology/p1113> "<http://www.opengis.net/def/crs/EPSG/0/4326>POINT(2.348611 48.853333)"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .
                        turtle = "<"+GEOVISTORY_RESOURCE.getUrl()+subjectId+"> <"+ ONTOME_PROPERTY.getUrl() +propertyId+"> \"<"+EPSG_4326.getUri()+">POINT("+ point.getX() +" "+ point.getY() +")\"^^<"+ OPENGIS_WKT.getUri() +"> .";
                    }
                    else if (timePrimitive != null) {
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

                            timePrimitiveTurtle.add("<" + GEOVISTORY_RESOURCE.getUrl() + subjectId + "ts> <" + ONTOME_PROPERTY.getUrl() + propertyId + "> <" + GEOVISTORY_RESOURCE.getUrl() + objectId + ">");
                            timePrimitiveTurtle.add("<" + GEOVISTORY_RESOURCE.getUrl() + objectId + "> <" + ONTOME_PROPERTY.getUrl() + propertyId + "i> <" + GEOVISTORY_RESOURCE.getUrl() + subjectId + "ts>");
                            timePrimitiveTurtle.add("<" + GEOVISTORY_RESOURCE.getUrl() + objectId + "> a <"+TIME.getUrl() +"DateTimeDescription>");
                            timePrimitiveTurtle.add("<" + GEOVISTORY_RESOURCE.getUrl() + objectId + "> <"+TIME.getUrl() +"hasTRS> <" + calendarSystemUri + ">");
                            timePrimitiveTurtle.add("<" + GEOVISTORY_RESOURCE.getUrl() + objectId + "> <"+TIME.getUrl() +"day> \"---" + d + "\"^^<"+TIME.getUrl() +"generalDay>");
                            timePrimitiveTurtle.add("<" + GEOVISTORY_RESOURCE.getUrl() + objectId + "> <"+TIME.getUrl() +"month> \"--" + m + "\"^^<"+TIME.getUrl() +"generalMonth>");
                            timePrimitiveTurtle.add("<" + GEOVISTORY_RESOURCE.getUrl() + objectId + "> <"+TIME.getUrl() +"year> \"-" + y + "\"^^<"+TIME.getUrl() +"generalYear>");
                            timePrimitiveTurtle.add("<" + GEOVISTORY_RESOURCE.getUrl() + objectId + "> <"+TIME.getUrl() +"hasTRS> <"+TIME.getUrl() + durationUnit + ">");
                            timePrimitiveTurtle.add("<" + GEOVISTORY_RESOURCE.getUrl() + objectId + "> <"+TIME.getUrl() +"year> \"" + label + "\"^^<"+XSD_STRING.getUri()+">");
                        }
                    }

                    else if (dimension != null) {
                        DecimalFormat format = new DecimalFormat("0.#");
                        var numericValue = format.format(dimension.getNumericValue()); //removes the trailing "0"
                        var fkMeasurementUnit = dimension.getFkMeasurementUnit();

                        dimensionTurtle.add("<"+ GEOVISTORY_RESOURCE.getUrl()+subjectId +"> <"+ ONTOME_PROPERTY.getUrl() +propertyId+"> <"+ GEOVISTORY_RESOURCE.getUrl()+objectId +">");
                        dimensionTurtle.add("<"+ GEOVISTORY_RESOURCE.getUrl()+objectId +"> <"+ ONTOME_PROPERTY.getUrl() +propertyId+"i> <"+ GEOVISTORY_RESOURCE.getUrl()+subjectId +">");
                        dimensionTurtle.add("<"+ GEOVISTORY_RESOURCE.getUrl()+objectId +"> <"+ ONTOME_PROPERTY.getUrl() +"78> \""+ numericValue +"\"^^<"+XSD.getUrl()+"decimal>");
                        dimensionTurtle.add("<"+ GEOVISTORY_RESOURCE.getUrl()+objectId +"> <"+ ONTOME_PROPERTY.getUrl() +"79> <"+ GEOVISTORY_RESOURCE.getUrl()+"i"+fkMeasurementUnit +">");
                        dimensionTurtle.add("<"+ GEOVISTORY_RESOURCE.getUrl()+"i"+fkMeasurementUnit +"> <"+ ONTOME_PROPERTY.getUrl() +"79i> <"+ GEOVISTORY_RESOURCE.getUrl()+objectId +">");

                    }

                    ProjectRdfKey k;
                    if (timePrimitiveTurtle.size() != 0) {
                        for (String item : timePrimitiveTurtle) {
                            k = ProjectRdfKey.newBuilder()
                                    .setProjectId(value.getProjectId())
                                    .setTurtle(item)
                                    .build();
                            result.add(KeyValue.pair(k, v));
                        }
                    }
                    else if (dimensionTurtle.size() != 0) {
                        for (String item : dimensionTurtle) {
                            k = ProjectRdfKey.newBuilder()
                                    .setProjectId(value.getProjectId())
                                    .setTurtle(item)
                                    .build();
                            result.add(KeyValue.pair(k, v));
                        }
                    }
                    else {
                        k = ProjectRdfKey.newBuilder()
                                .setProjectId(value.getProjectId())
                                .setTurtle(turtle)
                                .build();
                        result.add(KeyValue.pair(k, v));
                    }

                    // create a stream of key-value pairs

                    return result;
                }
        );

        /* SINK PROCESSORS */
        s.to(output.TOPICS.project_rdf,
                Produced.with(avroSerdes.ProjectRdfKey(), avroSerdes.ProjectRdfValue())
                        .withName(output.TOPICS.project_rdf + "-producer")
        );

        return new ProjectRdfReturnValue(builder, s);

    }

    public enum output {
        TOPICS;
        public final String project_rdf = Utils.tsPrefixed("project_rdf");
    }

}
