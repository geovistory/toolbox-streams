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

import java.util.Collection;

import static org.geovistory.toolbox.streams.lib.CommonUrls.*;
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

        var s = projectStatementWithEntityStream.map(
                (key, value) -> {

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
                    String[] tsTurtle = new String[9] ;
                    var julianUri = "<https://d-nb.info/gnd/4318310-4>";
                    var gregorianUri = "<http://www.opengis.net/def/uom/ISO-8601/0/Gregorian>";
                    var calendarSystemUri = "";
                    var julianDay = timePrimitive.getJulianDay();

                    // add the language triple
                    if (language != null) {
                        var lng = "";
                        if(language.getNotes() != null) {
                            lng = language.getNotes();
                        }
                        else lng = language.getPkLanguage();
                        //example: <http://geovistory.org/resource/i1761647> <https://ontome.net/ontology/p1112> "Italian"^^<http://www.w3.org/2001/XMLSchema#string> .
                        turtle = "<"+ GEOVISTORY_RESOURCE.getUrl() +subjectId+"> <"+ ONTOME_PROPERTY.getUrl() +propertyId+"> \""+lng+"\"^^<"+ XSD_STRING.getUrl() +"> .";
                    }
                    else if (appellation != null) {
                        //example: <http://geovistory.org/resource/i1761647> <https://ontome.net/ontology/p1113> "Foo"^^<http://www.w3.org/2001/XMLSchema#string> .
                        turtle = "<"+ GEOVISTORY_RESOURCE.getUrl() +subjectId+"> <"+ ONTOME_PROPERTY.getUrl() +propertyId+"> \""+appellation.getString()+"\"^^<"+ XSD_STRING.getUrl() +"> .";
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
                        var result = GeoUtils.bytesToPoint(java.util.Base64.getDecoder().decode(wkb));

                        //example: <http://geovistory.org/resource/i1761647> <https://ontome.net/ontology/p1113> "<http://www.opengis.net/def/crs/EPSG/0/4326>POINT(2.348611 48.853333)"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .
                        turtle = "<"+GEOVISTORY_RESOURCE.getUrl()+subjectId+"> <"+ ONTOME_PROPERTY.getUrl() +propertyId+"> \"<"+EPSG_4326.getUrl()+"\">POINT("+ result.getX() +" "+ result.getY() +")^^<"+ OPENGIS_WKT.getUrl() +"> .";
                    }
                    else if (timePrimitive != null) {
                        var julianYMD = TimeUtils.getYearMonthDay(julianDay, TimeUtils.CalendarType.julian).toString();
                        var gregorianYMD = TimeUtils.getYearMonthDay(julianDay, TimeUtils.CalendarType.gregorian).toString();
                        var ymd = "";
                        var y = "";
                        var m = "";
                        var d = "";

                        if (timePrimitive.getCalendar().equals("julian")) {
                            calendarSystemUri = julianUri;
                            ymd = julianYMD;
                        }
                        else{
                            calendarSystemUri = gregorianUri;
                            ymd = gregorianYMD;
                        }

                        y =  ymd.substring(0, 4);
                        m =  ymd.substring(4, 6);
                        d =  ymd.substring(6, 8);


                        tsTurtle[0] = "<"+ GEOVISTORY_RESOURCE.getUrl()+subjectId +"ts> <"+ ONTOME_PROPERTY.getUrl() +propertyId+"> <"+ GEOVISTORY_RESOURCE.getUrl()+objectId +">";
                        tsTurtle[1] = "<"+ GEOVISTORY_RESOURCE.getUrl()+objectId +"> <"+ ONTOME_PROPERTY.getUrl() +propertyId+"i> <"+ GEOVISTORY_RESOURCE.getUrl()+subjectId +"ts>";
                        tsTurtle[2] = "<"+ GEOVISTORY_RESOURCE.getUrl()+objectId +"> a <http://www.w3.org/2006/time#DateTimeDescription>";

                        tsTurtle[3] = "<"+ GEOVISTORY_RESOURCE.getUrl()+objectId +"> <http://www.w3.org/2006/time#hasTRS> <"+ calendarSystemUri +">";
                        tsTurtle[4] = "<"+ GEOVISTORY_RESOURCE.getUrl()+objectId +"> <http://www.w3.org/2006/time#day> \"---"+ d +"\"^^<http://www.w3.org/2006/time#generalDay>";
                        tsTurtle[5] = "<"+ GEOVISTORY_RESOURCE.getUrl()+objectId +"> <http://www.w3.org/2006/time#month> \"--"+ m +"\"^^<http://www.w3.org/2006/time#generalMonth>";
                        tsTurtle[6] = "<"+ GEOVISTORY_RESOURCE.getUrl()+objectId +"> <http://www.w3.org/2006/time#year> \""+ y +"\"^^<http://www.w3.org/2006/time#generalYear>";
                        tsTurtle[7] = "<"+ GEOVISTORY_RESOURCE.getUrl()+subjectId +"ts> <"+ ONTOME_PROPERTY.getUrl() +propertyId+"> <"+ GEOVISTORY_RESOURCE.getUrl()+objectId +">";
                        tsTurtle[8] = "<"+ GEOVISTORY_RESOURCE.getUrl()+subjectId +"ts> <"+ ONTOME_PROPERTY.getUrl() +propertyId+"> <"+ GEOVISTORY_RESOURCE.getUrl()+objectId +">";

                    }
                    var k = ProjectRdfKey.newBuilder()
                            .setProjectId(value.getProjectId())
                            .setTurtle(turtle)
                            .build();

                    // create a stream of key-value pairs
                    return KeyValue.pair(k, v);
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
