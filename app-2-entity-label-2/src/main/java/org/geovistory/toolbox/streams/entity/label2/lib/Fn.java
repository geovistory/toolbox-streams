package org.geovistory.toolbox.streams.entity.label2.lib;

import com.fasterxml.jackson.databind.ObjectMapper;
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
                .setDeleted(Utils.booleanIsEqualTrue(s.getDeleted$1()))
                .build();
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
