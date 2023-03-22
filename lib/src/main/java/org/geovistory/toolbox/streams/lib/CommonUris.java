package org.geovistory.toolbox.streams.lib;

public enum CommonUris {

    XSD_STRING("http://www.w3.org/2001/XMLSchema#string"),
    OPENGIS_WKT("http://www.opengis.net/ont/geosparql#wktLiteral"),
    EPSG_4326("http://www.opengis.net/def/crs/EPSG/0/4326"),
    JULIAN("https://d-nb.info/gnd/4318310-4"),
    GREGORIAN("http://www.opengis.net/def/uom/ISO-8601/0/Gregorian");

    private final String uri;

    CommonUris(String uri) {
        this.uri = uri;
    }

    public String getUri() {
        return uri;
    }
}

