package org.geovistory.toolbox.streams.entity.label3.lib;

import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.entity.label3.names.Processors;
import org.geovistory.toolbox.streams.entity.label3.names.PubTargets;

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
     * Creates a {@code ProjectLabelGroupKey} object based on the provided {@code LabelEdge} and {@code PubTargets}.
     *
     * <p>This method constructs a {@code ProjectLabelGroupKey} object using the attributes of the given {@code LabelEdge}
     * object and the {@code PubTargets}. The {@code projectId} is determined based on the {@code pubTarget} value. If the
     * {@code pubTarget} is {@code PubTargets.PC} or {@code PubTargets.TC}, the {@code projectId} is set to 0; otherwise, it
     * is set to the projectId of the given {@code LabelEdge}. The {@code language}, and {@code label} fields
     * of the {@code ProjectLabelGroupKey} are set to empty strings.</p>
     *
     * @param labelEdge The {@code LabelEdge} object representing the label edge.
     * @param pubTarget The {@code PubTargets} enum representing the publishing target.
     * @return The constructed {@code ProjectLabelGroupKey} object with the {@code projectId}, {@code entityId},
     * {@code language}, and {@code label} fields set as described.
     */
    public static ProjectLabelGroupKey createProjectLabelGroupKey(LabelEdge labelEdge, EntityLabel entityLabel, PubTargets pubTarget) {
        var projectId =
                pubTarget == PubTargets.PC
                        || pubTarget == PubTargets.TC
                        || pubTarget == PubTargets.TCL
                        || pubTarget == PubTargets.PCL
                        ? 0 : labelEdge.getProjectId();
        return ProjectLabelGroupKey.newBuilder()
                .setProjectId(projectId)
                .setEntityId(labelEdge.getSourceId())
                .setLanguage(entityLabel.getLanguage())
                .setLabel(entityLabel.getLabel()).build();
    }


    /**
     * Creates an {@code EntityLabelOperation} object based on the provided {@code EntityLabel} and deleted flag.
     * <p>
     * This method constructs an {@code EntityLabelOperation} object using the attributes of the given {@code EntityLabel}
     * object and a boolean flag indicating whether the entity label has been deleted or not. If the provided {@code EntityLabel}
     * is {@code null}, this method returns {@code null}.
     *
     * @param e       The {@code EntityLabel} object from which the label and language will be retrieved to construct the
     *                {@code EntityLabelOperation}.
     * @param deleted A boolean flag indicating whether the entity label has been deleted ({@code true}) or not ({@code false}).
     * @return The constructed {@code EntityLabelOperation} object with the label, language, and deleted status set as
     * provided, or {@code null} if the provided {@code EntityLabel} is {@code null}.
     */
    public static EntityLabelOperation createEntityLabelOperation(EntityLabel e, boolean deleted) {
        if (e == null) return null;
        return EntityLabelOperation.newBuilder()
                .setLabel(e.getLabel())
                .setLanguage(e.getLanguage())
                .setDeleted(deleted)
                .build();
    }

    /**
     * Creates an {@code EntityLabel} object based on the provided {@code EntityLabelOperation}.
     * <p>
     * This method constructs an {@code EntityLabel} object using the attributes of the given {@code EntityLabelOperation}
     * object. If the provided {@code EntityLabelOperation} is {@code null}, this method returns {@code null}.
     *
     * @param e The {@code EntityLabelOperation} object from which the label and language will be retrieved to construct the
     *          {@code EntityLabel}.
     * @return The constructed {@code EntityLabel} object with the label and language set as provided, or {@code null} if
     * the provided {@code EntityLabelOperation} is {@code null}.
     */
    public static EntityLabel createEntityLabel(EntityLabelOperation e) {
        if (e == null) return null;
        return EntityLabel.newBuilder()
                .setLabel(e.getLabel())
                .setLanguage(e.getLanguage())
                .build();
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
                // set visibility of source entity
                .setSourceProjectPublic(sourceVisibleInProjectPublic(edge))
                .setSourceCommunityPublic(sourceVisibleInCommunityPublic(edge))
                .setSourceCommunityToolbox(sourceVisibleInCommunityToolbox(edge))
                // set visibility of edge
                .setEdgeCommunityToolbox(edgeVisibleInCommunityToolbox(edge))
                // set other props
                .setProjectId(edge.getProjectId())
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
     * Determines if the EdgeValue is visible in the community's toolbox context.
     *
     * @param s The EdgeValue object to check visibility.
     * @return True if the EdgeValue is visible in the community's toolbox context, false otherwise.
     */
    private static boolean edgeVisibleInCommunityToolbox(EdgeValue s) {
        boolean sourceIsPublic = false;
        if (s.getSourceEntity() != null) {
            sourceIsPublic = s.getSourceEntity().getCommunityVisibilityToolbox();
        }
        boolean targetIsPublic;
        if (s.getTargetNode() != null && s.getTargetNode().getEntity() != null) {
            targetIsPublic = s.getTargetNode().getEntity().getCommunityVisibilityToolbox();
        } else {
            targetIsPublic = true;
        }

        return sourceIsPublic && targetIsPublic;
    }


    /**
     * Determines if the source entity of EdgeValue is visible in the project's public context.
     *
     * @param s The EdgeValue object to check visibility.
     * @return True if the EdgeValue is visible in the project's public context, false otherwise.
     */
    private static boolean sourceVisibleInProjectPublic(EdgeValue s) {
        boolean isPublic = false;
        if (s.getSourceProjectEntity() != null) {
            isPublic = s.getSourceProjectEntity().getProjectVisibilityDataApi();
        }
        return isPublic;
    }

    /**
     * Determines if the source entity of EdgeValue is visible in the community's public context.
     *
     * @param s The EdgeValue object to check visibility.
     * @return True if the EdgeValue is visible in the community's public context, false otherwise.
     */
    private static boolean sourceVisibleInCommunityPublic(EdgeValue s) {
        boolean isPublic = false;
        if (s.getSourceEntity() != null) {
            isPublic = s.getSourceEntity().getCommunityVisibilityDataApi();
        }
        return isPublic;
    }

    /**
     * Determines if the source entity of EdgeValue is visible in the community's toolbox context.
     *
     * @param s The EdgeValue object to check visibility.
     * @return True if the EdgeValue is visible in the community's toolbox context, false otherwise.
     */
    private static boolean sourceVisibleInCommunityToolbox(EdgeValue s) {
        boolean isPublic = false;
        if (s.getSourceEntity() != null) {
            isPublic = s.getSourceEntity().getCommunityVisibilityToolbox();
        }
        return isPublic;
    }

    /**
     * Extracts visibility of the source entity of a LabelEdge for the given PubTarget
     *
     * @param e LabelEdge
     * @param p PubTargets
     * @return true, if visible, else false;
     */
    public static boolean sourceVisibleInPublicationTarget(LabelEdge e, PubTargets p) {
        return switch (p) {
            case TP -> true;
            case TC, TCL -> e.getSourceCommunityToolbox();
            case PC, PCL -> e.getSourceCommunityPublic();
            case PP -> e.getSourceProjectPublic();
        };
    }

    /**
     * Extracts entity label operation childName for the given PubTarget
     *
     * @param p PubTargets
     * @return the child processor name
     */
    public static String getChildName(PubTargets p) {
        return switch (p) {
            case TP -> Processors.CREATE_LABEL_TOOLBOX_PROJECT;
            case TC -> Processors.CREATE_LABEL_TOOLBOX_COMMUNITY;
            case PC -> Processors.CREATE_LABEL_PUBLIC_COMMUNITY;
            case PP -> Processors.CREATE_LABEL_PUBLIC_PROJECT;
            case PCL -> Processors.CREATE_LANG_LABEL_PUBLIC_COMMUNITY;
            case TCL -> Processors.CREATE_LANG_LABEL_TOOLBOX_COMMUNITY;
        };
    }
}
