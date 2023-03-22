package org.geovistory.toolbox.streams.lib;

public enum UrlPrefixes {
    GEOVISTORY_RESOURCE("http://geovistory.org/resource/"),
    ONTOME_PROPERTY("https://ontome.net/ontology/p"),
    OWL("http://www.w3.org/2002/07/owl#"),
    RDFS("http://www.w3.org/2000/01/rdf-schema#"),
    RDF("http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
    XML("http://www.w3.org/XML/1998/namespace"),
    XSD("http://www.w3.org/2001/XMLSchema#"),
    FOAF("http://xmlns.com/foaf/0.1/"),
    OBDA("https://w3id.org/obda/vocabulary#"),
    GEO("http://www.opengis.net/ont/geosparql#"),
    TIME("http://www.w3.org/2006/time#");
    private final String url;

    UrlPrefixes(String url) {
        this.url = url;
    }

    public String getUrl() {
        return url;
    }
}

