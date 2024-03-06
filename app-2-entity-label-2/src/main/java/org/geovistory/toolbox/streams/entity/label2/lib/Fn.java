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
     * Join EntityValue and IprValue to ProjectEntityVisibilitiesValue
     *
     * @param entityValue EntityValue to join
     * @param iprValue    IprValue to join
     * @return ProjectEntityVisibilitiesValue
     */
    public static ProjectEntityVisibilitiesValue createProjectEntityValue(EntityValue entityValue, IprValue iprValue) {

        var v1Deleted = iprValue.getDeleted();
        var v2Deleted = entityValue.getDeleted();
        var notInProject = iprValue.getIsInProject() == null || !iprValue.getIsInProject();
        var deleted = v1Deleted || v2Deleted || notInProject;
        var communityCanSeeInToolbox = false;
        var communityCanSeeInDataApi = false;
        var communityCanSeeInWebsite = false;
        try {
            var communitVisibility = new ObjectMapper().readValue(entityValue.getCommunityVisibility(), CommunityVisibility.class);
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
        return ProjectEntityVisibilitiesValue.newBuilder()
                .setProjectId(iprValue.getFkProject())
                .setEntityId("i" + iprValue.getFkEntity())
                .setClassId(entityValue.getFkClass())
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
                .build();
    }

    /**
     * Transform ts.information.resource.Value to EntityValue.
     * Basically this removes metadata like timestamps and user info.
     *
     * @param e the ts.information.resource.Value to transform
     * @return EntityValue
     */
    public static EntityValue createEntityValue(Value e) {
        return EntityValue.newBuilder()
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
    public static IprJoinVal createIprJoinValue(ts.projects.info_proj_rel.Value ipr, EntityValue e) {
        return IprJoinVal.newBuilder().setIpr(createIprValue(ipr)).setE(e).build();
    }
}
