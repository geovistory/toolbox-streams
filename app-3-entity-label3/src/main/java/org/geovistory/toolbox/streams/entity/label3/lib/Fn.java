package org.geovistory.toolbox.streams.entity.label3.lib;

import org.geovistory.toolbox.streams.avro.*;

import java.time.Instant;

import static org.geovistory.toolbox.streams.lib.Utils.getLanguageFromId;

/**
 * Class to collect static functions
 */
public class Fn {

    /**
     * Creates a {@code ProjectLabelGroupKey} based on the provided {@code ProjectEntityKey}.
     *
     * @param k The {@code ProjectEntityKey} used to create the {@code ProjectLabelGroupKey}.
     * @return A {@code ProjectLabelGroupKey} representing the combination of project ID, entity ID,
     * language, and label (empty strings for language and label).
     * @throws NullPointerException if {@code k} is {@code null}.
     */
    public static ProjectLabelGroupKey createProjectLabelGroupKey(ProjectEntityKey k) {
        return ProjectLabelGroupKey.newBuilder()
                .setProjectId(k.getProjectId())
                .setEntityId(k.getEntityId())
                .setLanguage("")
                .setLabel("").build();
    }

    /**
     * Creates a {@code ProjectLabelGroupKey} based on the provided {@code ComLabelGroupKey}.
     *
     * @param k The {@code ComLabelGroupKey} used to create the {@code ProjectLabelGroupKey}.
     * @return A {@code ProjectLabelGroupKey} representing the combination of project ID (defaulted to 0),
     * entity ID, language, and label from the provided {@code ComLabelGroupKey}.
     * @throws NullPointerException if {@code k} is {@code null}.
     */
    public static ProjectLabelGroupKey createProjectLabelGroupKey(ComLabelGroupKey k) {
        return ProjectLabelGroupKey.newBuilder()
                .setProjectId(0)
                .setEntityId(k.getEntityId())
                .setLanguage(k.getLanguage())
                .setLabel(k.getLabel()).build();
    }


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
            Integer projectId,
            String sourceId,
            Integer propertyId,
            boolean isOutgoing,
            Float ordNum,
            String modifiedAt,
            String targetId
    ) {
        String ordNumStr = (ordNum == null) ? "z" : floatToHexString(ordNum);
        String modifiedAtStr = convertAndDivideTimestamp(modifiedAt);
        String[] strings = {
                ordNumStr,
                modifiedAtStr,
                targetId
        };
        return createLabelEdgePrefix3(classId, projectId, sourceId, propertyId, isOutgoing) + String.join("_", strings);
    }

    public static String convertAndDivideTimestamp(String inputTime) {
        try {
            // Parse the input string to Instant
            Instant instant = Instant.parse(inputTime);

            // Convert Instant to milliseconds since the epoch
            long milliseconds = instant.toEpochMilli();

            // Divide 1 by the number of milliseconds
            return (1.0f / milliseconds) + "";
        } catch (Exception e) {
            return "z";
        }
    }

    public static String createLabelEdgePrefix1(Integer classId) {

        return classId + "_";
    }

    public static String createLabelEdgePrefix2(Integer classId, Integer projectId) {
        return createLabelEdgePrefix1(classId) + projectId + "_";
    }

    public static String createLabelEdgePrefix3(Integer classId,
                                                Integer projectId,
                                                String sourceId,
                                                Integer propertyId,
                                                boolean isOutgoing) {
        String[] strings = {
                sourceId,
                propertyId.toString(),
                isOutgoing ? "o" : "i"
        };
        return createLabelEdgePrefix2(classId, projectId) + String.join("_", strings) + "_";
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
                e.getProjectId(),
                e.getSourceId(),
                e.getPropertyId(),
                e.getIsOutgoing(),
                e.getOrdNum(),
                e.getModifiedAt(),
                e.getTargetId()
        );
    }

    /**
     * Creates the ProjectEntityKey based on the source and project of the given Label edge.
     *
     * @param e LabelEdge
     * @return the ProjectEntityKey
     */
    public static ProjectEntityKey createProjectSourceEntityKey(LabelEdge e) {
        return ProjectEntityKey.newBuilder().setProjectId(e.getProjectId()).setEntityId(e.getSourceId()).build();
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

    /**
     * Creates a LabelEdge object based on the provided EdgeValue.
     *
     * @param edge The EdgeValue object to create LabelEdge from.
     * @return A LabelEdge object with properties extracted from the EdgeValue.
     */
    public static LabelEdge createLabelEdge(EdgeValue edge) {
        return LabelEdge.newBuilder()
                .setProjectId(edge.getProjectId())
                .setProjectPublic(visibleInProjectPublic(edge))
                .setCommunityPublic(visibleInCommunityPublic(edge))
                .setCommunityToolbox(visibleInCommunityToolbox(edge))
                .setSourceClassId(edge.getSourceEntity().getFkClass())
                .setSourceId(edge.getSourceId())
                .setPropertyId(edge.getPropertyId())
                .setIsOutgoing(edge.getIsOutgoing())
                .setOrdNum(edge.getOrdNum())
                .setModifiedAt(edge.getModifiedAt())
                .setTargetId(edge.getTargetId())
                .setTargetLabel(edge.getTargetNode().getLabel())
                .setTargetLabelLanguage(extractLabelLanguage(edge.getTargetNode()))
                .setTargetIsInProject(edge.getTargetProjectEntity() != null)
                .setDeleted(edge.getDeleted())
                .build();
    }

    /**
     * Extracts the language code from the provided NodeValue's language string.
     *
     * @param n The NodeValue object to extract language from.
     * @return The language code extracted from the NodeValue's language string, or "unknown" if not found.
     */
    private static String extractLabelLanguage(NodeValue n) {
        if (n.getLangString() != null) {
            var langCode = getLanguageFromId(n.getLangString().getFkLanguage());
            if (langCode != null) return langCode;
        }
        return "unknown";
    }

    /**
     * Determines if the EdgeValue is visible in the project's public context.
     *
     * @param s The EdgeValue object to check visibility.
     * @return True if the EdgeValue is visible in the project's public context, false otherwise.
     */
    private static boolean visibleInProjectPublic(EdgeValue s) {
        boolean isPublic = false;
        if (s.getSourceProjectEntity() != null) {
            isPublic = s.getSourceProjectEntity().getProjectVisibilityDataApi();
        }
        return isPublic;
    }

    /**
     * Determines if the EdgeValue is visible in the community's public context.
     *
     * @param s The EdgeValue object to check visibility.
     * @return True if the EdgeValue is visible in the community's public context, false otherwise.
     */
    private static boolean visibleInCommunityPublic(EdgeValue s) {
        boolean isPublic = false;
        if (s.getSourceEntity() != null) {
            isPublic = s.getSourceEntity().getCommunityVisibilityDataApi();
        }
        return isPublic;
    }

    /**
     * Determines if the EdgeValue is visible in the community's toolbox context.
     *
     * @param s The EdgeValue object to check visibility.
     * @return True if the EdgeValue is visible in the community's toolbox context, false otherwise.
     */
    private static boolean visibleInCommunityToolbox(EdgeValue s) {
        boolean isPublic = false;
        if (s.getSourceEntity() != null) {
            isPublic = s.getSourceEntity().getCommunityVisibilityToolbox();
        }
        return isPublic;
    }

}
