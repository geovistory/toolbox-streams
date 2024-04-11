package org.geovistory.toolbox.streams.entity.lib;

import org.geovistory.toolbox.streams.avro.EdgeValue;
import org.geovistory.toolbox.streams.avro.ProjectEntityKey;

import static org.geovistory.toolbox.streams.lib.Utils.convertAndDivideTimestamp;
import static org.geovistory.toolbox.streams.lib.Utils.floatToHexString;

public class Fn {

    /**
     * Creates the key used in state store of edges
     *
     * @param e LabelEdge
     * @return the key
     */
    public static String createEdgeSortableKey(EdgeValue e) {
        return createEdgeSortableKey(
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
     * Creates a key used in state store of edges based on the provided parameters.
     *
     * @param sourceId   The ID of the source.
     * @param projectId  The ID of the project.
     * @param propertyId The ID of the property.
     * @param isOutgoing Boolean indicating whether the edge is outgoing.
     * @param ordNum     The ordinal number of the edge.
     * @param modifiedAt The timestamp indicating when the edge was modified.
     * @param targetId   The ID of the target.
     * @return A string edge key that can be lexicographically sorted.
     */
    public static String createEdgeSortableKey(
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
        return createEdgePrefix(projectId, sourceId, propertyId, isOutgoing) + String.join("_", strings);
    }

    public static String createEdgePrefix(
            Integer projectId,
            String sourceId,
            Integer propertyId,
            boolean isOutgoing) {
        String[] strings = {
                projectId.toString(),
                sourceId,
                propertyId.toString(),
                isOutgoing ? "o" : "i"
        };
        return String.join("_", strings) + "_";
    }


    /**
     * Creates the ProjectEntityKey based on the source and project of the given edge.
     *
     * @param e EdgeValue
     * @return the ProjectEntityKey
     */
    public static ProjectEntityKey createProjectSourceEntityKey(EdgeValue e) {
        return ProjectEntityKey.newBuilder().setProjectId(e.getProjectId()).setEntityId(e.getSourceId()).build();
    }


}
