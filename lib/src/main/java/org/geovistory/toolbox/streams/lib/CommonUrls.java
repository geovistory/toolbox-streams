package org.geovistory.toolbox.streams.lib;

public enum CommonUrls {
    GEOVISTORY_RESOURCE("http://geovistory.org/resource/"),
    ONTOME_PROPERTY("https://ontome.net/ontology/p"),
    XSD_STRING("http://www.w3.org/2001/XMLSchema#string"),
    OPENGIS_WKT("http://www.opengis.net/ont/geosparql#wktLiteral"),
    EPSG_4326("http://www.opengis.net/def/crs/EPSG/0/4326");

    private final String url;

    CommonUrls(String url) {
        this.url = url;
    }

    public String getUrl() {
        return url;
    }
}

