package org.geovistory.toolbox.streams.project.items.lib;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.geovistory.toolbox.streams.avro.*;
import org.geovistory.toolbox.streams.lib.Utils;
import org.geovistory.toolbox.streams.lib.jsonmodels.CommunityVisibility;
import org.geovistory.toolbox.streams.lib.jsonmodels.ProjectVisibility;
import ts.information.resource.Value;

/**
 * Class to collect static functions
 */
public class Fn {

    /**
     * Join EntityProjectedValue and IprValue to EntityValue
     *
     * @param entityProjectedValue EntityProjectedValue to join
     * @param iprValue             IprValue to join
     * @return EntityValue
     */
    public static EntityValue createEntityValue(EntityProjectedValue entityProjectedValue, IprValue iprValue) {

        var v1Deleted = iprValue.getDeleted();
        var v2Deleted = entityProjectedValue.getDeleted();
        var notInProject = iprValue.getIsInProject() == null || !iprValue.getIsInProject();
        var deleted = v1Deleted || v2Deleted || notInProject;
        var communityCanSeeInToolbox = false;
        var communityCanSeeInDataApi = false;
        var communityCanSeeInWebsite = false;
        try {
            var communitVisibility = new ObjectMapper().readValue(entityProjectedValue.getCommunityVisibility(), CommunityVisibility.class);
            if (communitVisibility.toolbox) communityCanSeeInToolbox = true;
            if (communitVisibility.dataApi) communityCanSeeInDataApi = true;
            if (communitVisibility.website) communityCanSeeInWebsite = true;
        } catch (Exception e) {
            // ignoring because we use default values
        }
        var projectCanSeeInDataApi = false;
        var projectCanSeeInWebsite = false;
        try {
            var projectVisibility = new ObjectMapper().readValue(iprValue.getProjectVisibility(), ProjectVisibility.class);
            if (projectVisibility.dataApi) projectCanSeeInDataApi = true;
            if (projectVisibility.website) projectCanSeeInWebsite = true;
        } catch (Exception e) {
            // ignoring because we use default values
        }
        return EntityValue.newBuilder()
                .setProjectId(iprValue.getFkProject())
                .setEntityId("i" + iprValue.getFkEntity())
                .setClassId(entityProjectedValue.getFkClass())
                .setCommunityVisibilityToolbox(communityCanSeeInToolbox)
                .setCommunityVisibilityDataApi(communityCanSeeInDataApi)
                .setCommunityVisibilityWebsite(communityCanSeeInWebsite)
                .setProjectVisibilityDataApi(projectCanSeeInDataApi)
                .setProjectVisibilityWebsite(projectCanSeeInWebsite)
                .setDeleted(deleted)
                .build();
    }

    /**
     * Transform IprValue to ProjectEntityKey
     *
     * @param ipr ts.projects.info_proj_rel.Value
     * @return ProjectEntityKey
     */
    public static ProjectEntityKey createProjectEntityKey(IprValue ipr) {
        return ProjectEntityKey.newBuilder()
                .setProjectId(ipr.getFkProject())
                .setEntityId("i" + ipr.getFkEntity()).build();
    }


    /**
     * Transform projectId and entityId to ProjectEntityKey
     *
     * @return ProjectEntityKey
     */
    public static ProjectEntityKey createProjectEntityKey(int projectId, String entityId) {
        return ProjectEntityKey.newBuilder()
                .setProjectId(projectId)
                .setEntityId(entityId).build();
    }

    /**
     * Transform IprValue to ProjectStatementKey
     *
     * @param ipr ts.projects.info_proj_rel.Value
     * @return ProjectStatementKey
     */
    public static ProjectStatementKey createProjectStatementKey(IprValue ipr) {
        return ProjectStatementKey.newBuilder()
                .setProjectId(ipr.getFkProject())
                .setStatementId(ipr.getFkEntity()).build();
    }

    /**
     * Transform ts.projects.info_proj_rel.Value to IprValue
     * Basically this removes metadata like timestamps and user info.
     *
     * @param ipr ts.projects.info_proj_rel.Value
     * @return IprValue
     */
    public static IprValue createIprValue(ts.projects.info_proj_rel.Value ipr) {
        return IprValue.newBuilder()
                .setPkEntity(ipr.getPkEntity())
                .setFkEntity(ipr.getFkEntity())
                .setIsInProject(ipr.getIsInProject())
                .setFkProject(ipr.getFkProject())
                .setOrdNumOfDomain(ipr.getOrdNumOfDomain())
                .setOrdNumOfRange(ipr.getOrdNumOfRange())
                .setProjectVisibility(ipr.getProjectVisibility())
                .setModifiedAt(ipr.getTmspLastModification())
                .setDeleted(Utils.stringIsEqualTrue(ipr.getDeleted$1()))
                .build();
    }

    /**
     * Transform ts.information.resource.Value to EntityValue.
     * Basically this removes metadata like timestamps and user info.
     *
     * @param e the ts.information.resource.Value to transform
     * @return EntityValue
     */
    public static EntityProjectedValue createEntityProjectedValue(Value e) {
        return EntityProjectedValue.newBuilder()
                .setPkEntity(e.getPkEntity())
                .setFkClass(e.getFkClass())
                .setDeleted(Utils.stringIsEqualTrue(e.getDeleted$1()))
                .setCommunityVisibility(e.getCommunityVisibility()).build();
    }

    /**
     * Join ts.projects.info_proj_rel.Value and IprValue to IprJoinVal
     *
     * @param ipr IprValue to join
     * @param e   EntityValue to join
     * @return IprJoinVal
     */
    public static IprJoinVal createIprJoinValue(
            ts.projects.info_proj_rel.Value ipr,
            EntityProjectedValue e,
            StatementEnrichedValue s
    ) {
        return IprJoinVal.newBuilder().setIpr(createIprValue(ipr)).setE(e).setS(s).build();
    }

    /**
     * Join tIprValue and StatementEnrichedValue to StatementValue.
     *
     * @param s   StatementEnrichedValue to join
     * @param ipr IprValue to join
     * @return StatementValue
     */
    public static StatementValue createStatementValue(StatementEnrichedValue s, IprValue ipr) {
        return StatementValue.newBuilder()
                .setStatementId(ipr.getFkEntity())
                .setProjectId(ipr.getFkProject())
                .setProjectCount(0)
                .setOrdNumOfDomain(ipr.getOrdNumOfDomain() != null ? ipr.getOrdNumOfDomain().floatValue() : null)
                .setOrdNumOfRange(ipr.getOrdNumOfRange() != null ? ipr.getOrdNumOfRange().floatValue() : null)
                .setSubjectId(s.getSubjectId())
                .setPropertyId(s.getPropertyId())
                .setObjectId(s.getObjectId())
                .setSubjectClassId(s.getSubjectClassId())
                .setObjectClassId(s.getObjectClassId())
                .setSubjectLabel(s.getSubjectLabel())
                .setObjectLabel(s.getObjectLabel())
                .setSubject(s.getSubject())
                .setObject(s.getObject())
                .setModifiedAt(ipr.getModifiedAt())
                .setDeleted(Utils.booleanIsEqualTrue(s.getDeleted$1()) || !ipr.getIsInProject())
                .build();
    }

    /**
     * Join StatementValue and EntityValue to StatementWithSubValue
     *
     * @param s StatementValue to join
     * @param e EntityValue to join
     * @return StatementWithSubValue
     */
    public static StatementWithSubValue createStatementWithSubValue(
            StatementValue s,
            EntityValue e
    ) {
        return StatementWithSubValue.newBuilder()
                .setStatementId(s.getStatementId())
                .setProjectId(s.getProjectId())
                .setProjectCount(s.getProjectCount())
                .setOrdNumOfDomain(s.getOrdNumOfDomain())
                .setOrdNumOfRange(s.getOrdNumOfRange())
                .setSubjectId(s.getSubjectId())
                .setPropertyId(s.getPropertyId())
                .setObjectId(s.getObjectId())
                .setSubjectClassId(s.getSubjectClassId())
                .setObjectClassId(s.getObjectClassId())
                .setSubjectLabel(s.getSubjectLabel())
                .setObjectLabel(s.getObjectLabel())
                .setSubject(s.getSubject())
                .setObject(s.getObject())
                .setModifiedAt(s.getModifiedAt())
                .setDeleted(s.getDeleted())
                .setSubjectEntityValue(e)
                .build();
    }

    /**
     * Join StatementWithSubValue and EntityValue to StatementJoinValue
     *
     * @param s StatementWithSubValue to join
     * @param e EntityValue to join
     * @return StatementJoinValue
     */
    public static StatementJoinValue createStatementJoinValue(
            StatementWithSubValue s,
            EntityValue e
    ) {
        return StatementJoinValue.newBuilder()
                .setStatementId(s.getStatementId())
                .setProjectId(s.getProjectId())
                .setProjectCount(s.getProjectCount())
                .setOrdNumOfDomain(s.getOrdNumOfDomain())
                .setOrdNumOfRange(s.getOrdNumOfRange())
                .setSubjectId(s.getSubjectId())
                .setPropertyId(s.getPropertyId())
                .setObjectId(s.getObjectId())
                .setSubjectClassId(s.getSubjectClassId())
                .setObjectClassId(s.getObjectClassId())
                .setSubjectLabel(s.getSubjectLabel())
                .setObjectLabel(s.getObjectLabel())
                .setSubject(s.getSubject())
                .setObject(s.getObject())
                .setModifiedAt(s.getModifiedAt())
                .setDeleted(s.getDeleted())
                .setSubjectEntityValue(s.getSubjectEntityValue())
                .setObjectEntityValue(e)
                .build();
    }


    /**
     * Creates an outgoing EdgeVisibilityValue from StatementWithSubValue.
     *
     * @param s StatementWithSubValue
     * @return EdgeVisibilityValue
     */
    public static EdgeVisibilityValue createEdgeVisibility(StatementWithSubValue s) {
        return EdgeVisibilityValue.newBuilder()
                .setProjectPublic(visibleInProjectPublic(s))
                .setCommunityPublic(visibleInCommunityPublic(s))
                .setCommunityToolbox(visibleInCommunityToolbox(s))
                .setProjectId(s.getProjectId())
                .setStatementId(s.getStatementId())
                .setProjectCount(s.getProjectCount())
                .setOrdNum(s.getOrdNumOfRange())
                .setSourceId(s.getSubjectId())
                .setSourceEntity(s.getSubject().getEntity())
                .setSourceProjectEntity(s.getSubjectEntityValue())
                .setPropertyId(s.getPropertyId())
                .setIsOutgoing(true)
                .setTargetId(s.getObjectId())
                .setTargetNode(s.getObject())
                .setTargetProjectEntity(null)
                .setModifiedAt(s.getModifiedAt())
                .setDeleted(s.getDeleted())
                .build();
    }

    /**
     * Determines if the given statement can be published
     * in project rdf, based on the subject entity.
     *
     * @param s The StatementWithSubValue object representing the statement.
     * @return {@code true} if the subject entity is public; {@code false} otherwise.
     */
    private static boolean visibleInProjectPublic(StatementWithSubValue s) {
        boolean isPublic = false;
        if (s.getSubjectEntityValue() != null) {
            isPublic = s.getSubjectEntityValue().getProjectVisibilityDataApi();
        }
        return isPublic;
    }

    /**
     * Determines if the given statement can be published
     * in community public rdf, based on the subject entity.
     *
     * @param s The StatementWithSubValue object representing the statement.
     * @return {@code true} if the subject entity is public; {@code false} otherwise.
     */
    private static boolean visibleInCommunityPublic(StatementWithSubValue s) {
        boolean isPublic = false;
        if (s.getSubject() != null && s.getSubject().getEntity() != null) {
            isPublic = s.getSubject().getEntity().getCommunityVisibilityDataApi();
        }
        return isPublic;
    }

    /**
     * Determines if the given statement can be published
     * in community toolbox rdf, based on the subject entity.
     *
     * @param s The StatementWithSubValue object representing the statement.
     * @return {@code true} if the subject entity is public; {@code false} otherwise.
     */
    private static boolean visibleInCommunityToolbox(StatementWithSubValue s) {
        boolean isPublic = false;
        if (s.getSubject() != null && s.getSubject().getEntity() != null) {
            isPublic = s.getSubject().getEntity().getCommunityVisibilityToolbox();
        }
        return isPublic;
    }

    /**
     * Creates an outgoing EdgeValue from EdgeVisibilityValue.
     *
     * @param e EdgeVisibilityValue
     * @return EdgeValue
     */
    public static EdgeValue createEdge(EdgeVisibilityValue e) {
        return EdgeValue.newBuilder()
                .setProjectId(e.getProjectId())
                .setStatementId(e.getStatementId())
                .setProjectCount(e.getProjectCount())
                .setOrdNum(e.getOrdNum())
                .setSourceId(e.getSourceId())
                .setSourceEntity(e.getSourceEntity())
                .setSourceProjectEntity(e.getSourceProjectEntity())
                .setPropertyId(e.getPropertyId())
                .setIsOutgoing(e.getIsOutgoing())
                .setTargetId(e.getTargetId())
                .setTargetNode(e.getTargetNode())
                .setTargetProjectEntity(e.getTargetProjectEntity())
                .setModifiedAt(e.getModifiedAt())
                .setDeleted(e.getDeleted())
                .build();
    }

    /**
     * Creates an outgoing EdgeVisibilityValue from StatementJoinValue.
     *
     * @param s StatementJoinValue
     * @return EdgeVisibilityValue
     */
    public static EdgeVisibilityValue createOutgoingEdge(StatementJoinValue s) throws RuntimeException {
        if (s.getSubject() == null || s.getSubject().getEntity() == null || s.getObject() == null) {
            throw new RuntimeException("Could not transform StatementJoinValue to EdgeVisibilityValue: subject.entity and object needed.");
        }

        return EdgeVisibilityValue.newBuilder()
                .setProjectPublic(visibleInProjectPublic(s))
                .setCommunityPublic(visibleInCommunityPublic(s))
                .setCommunityToolbox(visibleInCommunityToolbox(s))
                .setProjectId(s.getProjectId())
                .setStatementId(s.getStatementId())
                .setProjectCount(s.getProjectCount())
                .setOrdNum(s.getOrdNumOfRange())
                .setSourceId(s.getSubjectId())
                .setSourceEntity(s.getSubject().getEntity())
                .setSourceProjectEntity(s.getSubjectEntityValue())
                .setPropertyId(s.getPropertyId())
                .setIsOutgoing(true)
                .setTargetId(s.getObjectId())
                .setTargetNode(s.getObject())
                .setTargetProjectEntity(s.getObjectEntityValue())
                .setModifiedAt(s.getModifiedAt())
                .setDeleted(s.getDeleted())
                .build();
    }

    /**
     * Creates an incoming EdgeVisibilityValue from StatementJoinValue.
     *
     * @param s StatementJoinValue
     * @return EdgeVisibilityValue
     */
    public static EdgeVisibilityValue createIncomingEdge(StatementJoinValue s) throws RuntimeException {
        if (s.getObject() == null || s.getObject().getEntity() == null || s.getSubject() == null) {
            throw new RuntimeException("Could not transform StatementJoinValue to EdgeValue: object.entity and subject needed.");
        }

        return EdgeVisibilityValue.newBuilder()
                .setProjectPublic(visibleInProjectPublic(s))
                .setCommunityPublic(visibleInCommunityPublic(s))
                .setCommunityToolbox(visibleInCommunityToolbox(s))
                .setProjectId(s.getProjectId())
                .setStatementId(s.getStatementId())
                .setProjectCount(s.getProjectCount())
                .setOrdNum(s.getOrdNumOfDomain())
                .setSourceId(s.getObjectId())
                .setSourceEntity(s.getObject().getEntity())
                .setSourceProjectEntity(s.getObjectEntityValue())
                .setPropertyId(s.getPropertyId())
                .setIsOutgoing(false)
                .setTargetId(s.getSubjectId())
                .setTargetNode(s.getSubject())
                .setTargetProjectEntity(s.getSubjectEntityValue())
                .setModifiedAt(s.getModifiedAt())
                .setDeleted(s.getDeleted())
                .build();
    }


    /**
     * Determines if the given statement can be published
     * project public rdf, based on both the subject and object entities
     *
     * @param s The StatementJoinValue object representing the statement.
     * @return {@code true} if both the subject and object entities are public;
     * {@code false} otherwise.
     */
    private static boolean visibleInProjectPublic(StatementJoinValue s) {
        boolean subjectIsPublic = false;
        if (s.getSubjectEntityValue() != null) {
            subjectIsPublic = s.getSubjectEntityValue().getProjectVisibilityDataApi();
        }

        boolean objectIsPublic = false;
        if (s.getSubjectEntityValue() != null) {
            objectIsPublic = s.getObjectEntityValue().getProjectVisibilityDataApi();
        }

        return subjectIsPublic && objectIsPublic;
    }


    /**
     * Determines if the given statement can be published
     * in community public rdf, based on both the subject and object entities.
     *
     * @param s The StatementJoinValue object representing the statement.
     * @return {@code true} if both entities are public; {@code false} otherwise.
     */
    private static boolean visibleInCommunityPublic(StatementJoinValue s) {
        boolean subjectIsPublic = false;
        if (s.getSubject() != null && s.getSubject().getEntity() != null) {
            subjectIsPublic = s.getSubject().getEntity().getCommunityVisibilityDataApi();
        }
        boolean objectIsPublic = false;
        if (s.getObject() != null && s.getObject().getEntity() != null) {
            objectIsPublic = s.getSubject().getEntity().getCommunityVisibilityDataApi();
        }
        return subjectIsPublic && objectIsPublic;
    }

    /**
     * Determines if the given statement can be published
     * in community toolbox rdf, based on both the subject and object entities.
     *
     * @param s The StatementJoinValue object representing the statement.
     * @return {@code true} if both entities are public; {@code false} otherwise.
     */
    private static boolean visibleInCommunityToolbox(StatementJoinValue s) {
        boolean subjectIsPublic = false;
        if (s.getSubject() != null && s.getSubject().getEntity() != null) {
            subjectIsPublic = s.getSubject().getEntity().getCommunityVisibilityToolbox();
        }
        boolean objectIsPublic = false;
        if (s.getObject() != null && s.getObject().getEntity() != null) {
            objectIsPublic = s.getSubject().getEntity().getCommunityVisibilityToolbox();
        }
        return subjectIsPublic && objectIsPublic;
    }


    /**
     * Creates the key of an edge
     *
     * @param projectId  Id of project
     * @param sourceId   Id of source entity
     * @param propertyId Id of property
     * @param targetId   Id of target entity
     * @param isOutgoing True if source is subject
     * @return key of edge
     */
    public static String createEdgeKey(int projectId, String sourceId, int propertyId, boolean isOutgoing, String targetId) {
        return projectId + "_" + sourceId + "_" + propertyId + "_" + (isOutgoing ? "o" : "i") + "_" + targetId;
    }

    /**
     * Creates the key of an edge
     *
     * @param e EdgeValue
     * @return the key
     */
    public static String createEdgeKey(EdgeValue e) {
        return createEdgeKey(e.getProjectId(), e.getSourceId(), e.getPropertyId(), e.getIsOutgoing(), e.getTargetId());
    }

    /**
     * Creates the key of an edge
     *
     * @param e EdgeVisibilityValue
     * @return the key
     */
    public static String createEdgeKey(EdgeVisibilityValue e) {
        return createEdgeKey(e.getProjectId(), e.getSourceId(), e.getPropertyId(), e.getIsOutgoing(), e.getTargetId());
    }


    /**
     * Creates the ProjectEntityKey of the source entity in the given edge
     *
     * @param value EdgeValue
     * @return the ProjectEntityKey
     */
    public static ProjectEntityKey createProjectEntityKeyOfSource(EdgeValue value) {
        return ProjectEntityKey.newBuilder().setEntityId(value.getSourceId()).setProjectId(value.getProjectId()).build();
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
     * Determines the operation to be performed based on the provided parameters.
     * The return value can be used to forward records to publication targets.
     *
     * @param boolStore  The key-value store used to check and update the presence of the target key.
     * @param record     The record containing the value to be analyzed.
     * @param visibility The extractor for determining visibility of the value in the publication target.
     * @param targetKey  The key to be checked and updated in the key-value store.
     * @param modified   A boolean indicating whether the value has been modified.
     * @param deleted    A boolean indicating whether the value has been marked for deletion.
     * @param <K>        The type of the key in the record.
     * @param <V>        The type of the value in the record.
     * @return An {@link Op} representing the operation to be performed.
     */
    public static <K, V> Op getOperation(
            KeyValueStore<String, Boolean> boolStore,
            Record<K, V> record,
            ExtractVisiblity<V> visibility,
            String targetKey,
            boolean modified,
            boolean deleted) {

        var v = record.value();

        // get info if this item is present on target
        var wasInSlug = boolStore.get(targetKey);
        wasInSlug = wasInSlug != null && wasInSlug;
        var visibleInSlug = visibility.get(v);

        // if it has to be deleted
        if (wasInSlug && (!visibleInSlug || deleted)) {
            // mark as not in target
            boolStore.put(targetKey, false);
            // forward message for deletion
            return Op.DELETE;
        }
        // if it has to be added
        else if (!wasInSlug && visibleInSlug && !deleted) {
            // mark as present in target
            boolStore.put(targetKey, true);
            // forward message for adding
            return Op.INSERT;
        }
        // if the class changed
        else if (visibleInSlug && !deleted && modified) {
            // forward update
            return Op.UPDATE;
        }

        return Op.NOTHING;
    }
}
