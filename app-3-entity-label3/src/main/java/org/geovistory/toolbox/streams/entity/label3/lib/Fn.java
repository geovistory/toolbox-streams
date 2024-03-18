package org.geovistory.toolbox.streams.entity.label3.lib;

import org.geovistory.toolbox.streams.avro.LabelEdge;

/**
 * Class to collect static functions
 */
public class Fn {


    /**
     * Creates a key used in state store of label-edges-by-source based on the provided parameters.
     *
     * @param classId    The ID of the class.
     * @param sourceId   The ID of the source.
     * @param projectId  The ID of the project.
     * @param propertyId The ID of the property.
     * @param isOutgoing Boolean indicating whether the edge is outgoing.
     * @param ordNum     The ordinal number of the edge.
     * @param modifiedAt The timestamp indicating when the edge was modified.
     * @param targetId   The ID of the target.
     * @return A string representing the edge key.
     */
    public static String createLabelEdgeSourceKey(
            Integer classId,
            String sourceId,
            Integer projectId,
            Integer propertyId,
            boolean isOutgoing,
            Float ordNum,
            String modifiedAt,
            String targetId
    ) {
        String ordNumStr = (ordNum == null) ? "zzzzzzzz" : floatToHexString(ordNum);
        String modifiedAtStr = modifiedAt == null ? "Z" : modifiedAt;
        String[] strings = {
                classId.toString(),
                sourceId,
                projectId.toString(),
                propertyId.toString(),
                isOutgoing ? "o" : "i",
                ordNumStr,
                modifiedAtStr,
                targetId
        };
        return String.join("_", strings);
    }

    /**
     * Creates the key used in state store of label-edges-by-source
     *
     * @param e LabelEdge
     * @return the key
     */
    public static String createLabelEdgeSourceKey(LabelEdge e) {
        return createLabelEdgeSourceKey(
                e.getSourceClassId(),
                e.getSourceId(),
                e.getProjectId(),
                e.getPropertyId(),
                e.getIsOutgoing(),
                e.getOrdNum(),
                e.getModifiedAt(),
                e.getTargetId()
        );
    }

    /**
     * Converts a float to its hexadecimal representation as a string.
     * This method can be used to create strings that can be lexicographically ordered,
     * as long as the input float is not negative.
     *
     * @param f the float value to convert to hexadecimal
     * @return the hexadecimal representation of the float as a string
     */
    public static String floatToHexString(float f) {
        // Convert float to hexadecimal representation
        int floatBits = Float.floatToIntBits(f);
        String hexString = Integer.toHexString(floatBits);

        // Pad the hexadecimal string with zeros to ensure it has 8 characters
        hexString = String.format("%8s", hexString).replace(' ', '0');
        return hexString;
    }

}
