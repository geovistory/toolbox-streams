package org.geovistory.toolbox.streams.entity.label3.testlib;

import org.geovistory.toolbox.streams.avro.*;

import static java.lang.Integer.parseInt;

public class MockEdges {


    public static EdgeValue createEdgeWithoutProjectEntity(

            Integer project_id,
            Integer source_pk_entity,
            Integer source_fk_class,
            Float ord_num,
            Integer property_id,
            Boolean is_outgoing,
            NodeValue target_node,
            String modified_at,
            Boolean deleted
    ) {
        String target_id = target_node.getId();
        return createEdge(
                project_id,
                source_pk_entity,
                source_fk_class,
                ord_num,
                property_id,
                is_outgoing,
                target_node,
                null,
                target_id,
                modified_at,
                deleted
        );

    }

    public static EdgeValue createEdgeWithProjectEntity(
            Integer project_id,
            Integer source_pk_entity,
            Integer source_fk_class,
            Float ord_num,
            Integer property_id,
            Boolean is_outgoing,
            String target_entity_id,
            Integer target_class_id,
            Integer target_project_id,
            Boolean target_deleted,
            String modified_at,
            Boolean deleted
    ) {
        EntityValue target_project_entity = createEntityValue(
                target_entity_id,
                target_class_id,
                target_project_id,
                target_deleted
        );
        String target_id = target_project_entity.getEntityId();
        NodeValue target_node = nodeValueWithEntity(
                parseInt(target_project_entity.getEntityId().replace("i", "")),
                target_project_entity.getClassId(),
                target_project_entity.getCommunityVisibilityToolbox(),
                target_project_entity.getCommunityVisibilityDataApi(),
                target_project_entity.getCommunityVisibilityWebsite()
        );
        return createEdge(
                project_id,
                source_pk_entity,
                source_fk_class,
                ord_num,
                property_id,
                is_outgoing,
                target_node,
                target_project_entity,
                target_id,
                modified_at,
                deleted
        );

    }

    private static EdgeValue createEdge(

            Integer project_id,
            Integer source_pk_entity,
            Integer source_fk_class,
            Float ord_num,
            Integer property_id,
            Boolean is_outgoing,
            NodeValue target_node,
            EntityValue target_project_entity,
            String target_id,
            String modified_at,
            Boolean deleted
    ) {
        // constants
        Integer statement_id = 0;
        Boolean source_community_visibility_toolbox = true;
        Boolean source_community_visibility_data_api = true;
        Boolean source_community_visibility_website = true;
        Boolean source_project_visibility_data_api = true;
        Boolean source_project_visibility_website = true;
        Boolean source_project_entity_deleted = true;
        Integer project_count = 0;


        String source_id = "i" + source_pk_entity;
        Entity source_entity = new Entity(
                source_pk_entity,
                source_fk_class,
                source_community_visibility_toolbox,
                source_community_visibility_data_api,
                source_community_visibility_website
        );
        EntityValue source_project_entity = new EntityValue(
                project_id,
                source_id,
                source_fk_class,
                source_community_visibility_toolbox,
                source_community_visibility_data_api,
                source_community_visibility_website,
                source_project_visibility_data_api,
                source_project_visibility_website,
                source_project_entity_deleted
        );

        return new EdgeValue(
                project_id,
                statement_id,
                project_count,
                ord_num,
                source_id,
                source_project_entity,
                source_entity,
                property_id,
                is_outgoing,
                target_id,
                target_project_entity,
                target_node,
                modified_at,
                deleted
        );

    }

    public static NodeValue nodeValueWithLabel(
            Integer pk_entity,
            Integer fk_class,
            String label
    ) {
        String id = "i" + pk_entity;
        return NodeValue.newBuilder()
                .setId(id)
                .setClassId(fk_class)
                .setLabel(label).build();
    }

    public static NodeValue nodeValueWithLangString(
            Integer pk_entity,
            Integer fk_class,
            String label,
            Integer language
    ) {
        String id = "i" + pk_entity;
        return NodeValue.newBuilder()
                .setId(id)
                .setClassId(fk_class)
                .setLangString(LangString.newBuilder().setString(label).setFkLanguage(language).setPkEntity(pk_entity).build())
                .setLabel(label).build();
    }

    public static NodeValue nodeValueWithLanguage(
            Integer pk_entity
    ) {
        String id = "i" + pk_entity;
        return NodeValue.newBuilder()
                .setId(id)
                .setClassId(54)
                .setLanguage(Language.newBuilder().setPkLanguage(pk_entity.toString()).setPkEntity(pk_entity).build())
                .setLabel("German").build();
    }

    private static NodeValue nodeValueWithEntity(
            Integer pk_entity,
            Integer fk_class,
            Boolean community_visibility_toolbox,
            Boolean community_visibility_data_api,
            Boolean community_visibility_website
    ) {
        Entity target_node_entity = new Entity(
                pk_entity,
                fk_class,
                community_visibility_toolbox,
                community_visibility_data_api,
                community_visibility_website
        );
        String id = "i" + pk_entity;

        return NodeValue.newBuilder()
                .setId(id)
                .setClassId(fk_class)
                .setEntity(target_node_entity).build();
    }


    private static EntityValue createEntityValue(
            String entity_id,
            Integer class_id,
            Integer project_id,
            Boolean deleted
    ) {

        return new EntityValue(
                project_id,
                entity_id,
                class_id,
                true,
                true,
                true,
                true,
                true,
                deleted
        );
    }

    public static String createEdgeKey(EdgeValue e) {
        return createEdgeKey(e.getProjectId(), e.getSourceId(), e.getPropertyId(), e.getIsOutgoing(), e.getTargetId());
    }

    public static String createEdgeKey(int projectId, String sourceId, int propertyId, boolean isOutgoing, String targetId) {
        return projectId + "_" + sourceId + "_" + propertyId + "_" + (isOutgoing ? "o" : "i") + "_" + targetId;
    }

}
