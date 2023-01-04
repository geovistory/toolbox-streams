package org.geovistory.toolbox.streams.lib;


import io.debezium.data.geometry.Point;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class GeoUtils {
    private static final int WKB_POINT = 1; // type constant
    private static final int WKB_POINT_SIZE = (1 + 4 + 8 + 8); // fixed size


    /**
     * Creates a GeoPoint as OGC Well-known bites (wkb) representation in the form
     * of a ByteBuffer.
     *
     * @param x e.g. 1.23
     * @param y e.g. 3.0
     * @return Point as wkb ByteBuffer
     */
    public static ByteBuffer pointToByteBuffer(double x, double y) {
        ByteBuffer wkb = ByteBuffer.allocate(WKB_POINT_SIZE);
        wkb.put((byte) 1); // BOM
        wkb.order(ByteOrder.LITTLE_ENDIAN);

        wkb.putInt(WKB_POINT);
        wkb.putDouble(x);
        wkb.putDouble(y);
        wkb.rewind();
        return wkb;
    }

    /**
     * Parses wkb byte[] to {x,y}.
     *
     * @param wkb OGC WKB point geometry
     * @return {x,y}
     */
    public static double[] wkbToXY(byte[] wkb) {
        return Point.parseWKBPoint(wkb);
    }


    /**
     * Parses wkb ByteBuffer to Point.
     *
     * @param wkb OGC WKB point geometry
     * @return Point
     */
    public static double[] wkbToXY(ByteBuffer wkb) {
        return wkbToXY(wkb.array());
    }

}
