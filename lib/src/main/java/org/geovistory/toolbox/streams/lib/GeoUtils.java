package org.geovistory.toolbox.streams.lib;


import org.postgis.Point;
import org.postgis.binary.BinaryParser;
import org.postgis.binary.BinaryWriter;

import java.nio.ByteBuffer;

public class GeoUtils {

    public static BinaryWriter writer = new BinaryWriter();
    public static BinaryParser parser = new BinaryParser();

    /**
     * Creates Extended Well-known bytes ByteBuffer for a point geometry
     *
     * @param x e.g. 1.23
     * @param y e.g. 3.0
     * @param srid e.g. 4326
     * @return PostGIS Point
     */
    public static ByteBuffer pointToBytes(double x, double y, int srid) {
        var point = new Point();
        point.setSrid(srid);
        point.setX(x);
        point.setY(y);
        var bytes = writer.writeBinary(point);
        return ByteBuffer.wrap(bytes);
    }

    /**
     * Parses Extended Well-known binary byte[] to {x,y}.
     *
     * @param bytes Extended WKB point geometry
     * @return {x,y}
     */
    public static Point bytesToPoint(byte[] bytes) {
        var g = parser.parse(bytes);
        return g.getFirstPoint();
    }


    /**
     * Parses bytes ByteBuffer to Point.
     *
     * @param bytes Extended WKB point geometry
     * @return Point
     */
    public static Point bytesToPoint(ByteBuffer bytes) {
        return bytesToPoint(bytes.array());
    }

}
