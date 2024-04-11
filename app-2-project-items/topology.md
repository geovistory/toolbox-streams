```mermaid
graph TD
ts.information.resource[ts.information.resource] --> ts.information.resource-source(ts.information.resource-<br>source)
REPARTITIONED_E_BY_PK_SINK(REPARTITIONED_E_BY_PK_SINK) --> ts_p_items_6_e_repartitioned[ts_p_items_6_e_repartitioned]
ts.projects.info_proj_rel[ts.projects.info_proj_rel] --> ts.projects.info_proj_rel-source(ts.projects.info_proj_rel-<br>source)
REPARTITIONED_IPR_BY_FKE_SINK(REPARTITIONED_IPR_BY_FKE_SINK) --> ts_p_items_6_ipr_repartitioned[ts_p_items_6_ipr_repartitioned]
ts_statement_object_statement_with_entity[ts_statement_object_statement_with_entity] --> ts_statement_object_statement_with_literal-source(ts_statement_object_statement_with_literal-<br>source)
ts_statement_object_statement_with_literal[ts_statement_object_statement_with_literal] --> ts_statement_object_statement_with_literal-source(ts_statement_object_statement_with_literal-<br>source)
REPARTITIONED_S_BY_PK_SINK(REPARTITIONED_S_BY_PK_SINK) --> ts_p_items_6_s_repartitioned[ts_p_items_6_s_repartitioned]
ts_p_items_6_project_entity[ts_p_items_6_project_entity] --> PROJECT_ENTITY_SOURCE(PROJECT_ENTITY_SOURCE)
ts_p_items_6_ipr_repartitioned[ts_p_items_6_ipr_repartitioned] --> REPARTITIONED_IPR_BY_FKE_SOURCE(REPARTITIONED_IPR_BY_FKE_SOURCE)
STOCK_PE(STOCK_PE) --> pe-store[(pe-<br>store)]
e-store[(e-<br>store)] --> JOIN_IPR(JOIN_IPR)
s-store[(s-<br>store)] --> JOIN_IPR(JOIN_IPR)
ipr-store[(ipr-<br>store)] --> JOIN_IPR(JOIN_IPR)
ts_p_items_6_project_statement_repartitioned_by_subject[ts_p_items_6_project_statement_repartitioned_by_subject] --> PROJECT_STATEMENT_REPARTITIONED_BY_SUB_SOURCE(PROJECT_STATEMENT_REPARTITIONED_BY_SUB_SOURCE)
s-sub-store[(s-<br>sub-<br>store)] --> JOIN_PE_S_SUB(JOIN_PE_S_SUB)
pe-store[(pe-<br>store)] --> JOIN_S_SUB_PE(JOIN_S_SUB_PE)
s-sub-store[(s-<br>sub-<br>store)] --> JOIN_S_SUB_PE(JOIN_S_SUB_PE)
ts_p_items_6_project_statement_with_ob_by_pk[ts_p_items_6_project_statement_with_ob_by_pk] --> PROJECT_S_OB_BY_PK_SOURCE(PROJECT_S_OB_BY_PK_SOURCE)
ts_p_items_6_project_statement_with_sub_by_pk[ts_p_items_6_project_statement_with_sub_by_pk] --> PROJECT_S_SUB_BY_PK_SOURCE(PROJECT_S_SUB_BY_PK_SOURCE)
s-complete-store[(s-<br>complete-<br>store)] --> JOIN_OB_WITH_SUB(JOIN_OB_WITH_SUB)
s-ob-store[(s-<br>ob-<br>store)] --> JOIN_OB_WITH_SUB(JOIN_OB_WITH_SUB)
s-complete-store[(s-<br>complete-<br>store)] --> JOIN_SUB_WITH_OB(JOIN_SUB_WITH_OB)
ts_p_items_6_e_repartitioned[ts_p_items_6_e_repartitioned] --> REPARTITIONED_E_BY_PK_SOURCE(REPARTITIONED_E_BY_PK_SOURCE)
FORK_EDGES(FORK_EDGES) --> entity-bool-store[(entity-<br>bool-<br>store)]
FORK_EDGES(FORK_EDGES) --> edge-visibility-store[(edge-<br>visibility-<br>store)]
e-store[(e-<br>store)] --> JOIN_E(JOIN_E)
ipr-store[(ipr-<br>store)] --> JOIN_E(JOIN_E)
ts_p_items_6_s_repartitioned[ts_p_items_6_s_repartitioned] --> REPARTITIONED_S_BY_PK_SOURCE(REPARTITIONED_S_BY_PK_SOURCE)
FORK_ENTITIES(FORK_ENTITIES) --> entity-bool-store[(entity-<br>bool-<br>store)]
FORK_ENTITIES(FORK_ENTITIES) --> entity-count-store[(entity-<br>count-<br>store)]
s-store[(s-<br>store)] --> JOIN_S(JOIN_S)
ipr-store[(ipr-<br>store)] --> JOIN_S(JOIN_S)
ts_p_items_6_project_statement_repartitioned_by_object[ts_p_items_6_project_statement_repartitioned_by_object] --> PROJECT_STATEMENT_REPARTITIONED_BY_OB_SOURCE(PROJECT_STATEMENT_REPARTITIONED_BY_OB_SOURCE)
s-ob-store[(s-<br>ob-<br>store)] --> JOIN_PE_S_OB(JOIN_PE_S_OB)
pe-store[(pe-<br>store)] --> JOIN_S_OB_PE(JOIN_S_OB_PE)
s-ob-store[(s-<br>ob-<br>store)] --> JOIN_S_OB_PE(JOIN_S_OB_PE)
PUBLIC_CREATE_COMMUNITY_EDGES(PUBLIC_CREATE_COMMUNITY_EDGES) --> edge-sum-store[(edge-<br>sum-<br>store)]
PUBLIC_CREATE_COMMUNITY_EDGES(PUBLIC_CREATE_COMMUNITY_EDGES) --> edge-count-store[(edge-<br>count-<br>store)]
PUBLIC_CREATE_COMMUNITY_EDGES(PUBLIC_CREATE_COMMUNITY_EDGES) --> edge-bool-store[(edge-<br>bool-<br>store)]
TOOLBOX_CREATE_COMMUNITY_EDGES(TOOLBOX_CREATE_COMMUNITY_EDGES) --> edge-sum-store[(edge-<br>sum-<br>store)]
TOOLBOX_CREATE_COMMUNITY_EDGES(TOOLBOX_CREATE_COMMUNITY_EDGES) --> edge-count-store[(edge-<br>count-<br>store)]
TOOLBOX_CREATE_COMMUNITY_EDGES(TOOLBOX_CREATE_COMMUNITY_EDGES) --> edge-bool-store[(edge-<br>bool-<br>store)]
PROJECT_ENTITY_SINK(PROJECT_ENTITY_SINK) --> ts_p_items_6_project_entity[ts_p_items_6_project_entity]
PROJECT_STATEMENT_REPARTITIONED_BY_OB_SINK(PROJECT_STATEMENT_REPARTITIONED_BY_OB_SINK) --> ts_p_items_6_project_statement_repartitioned_by_object[ts_p_items_6_project_statement_repartitioned_by_object]
PROJECT_STATEMENT_REPARTITIONED_BY_SUB_SINK(PROJECT_STATEMENT_REPARTITIONED_BY_SUB_SINK) --> ts_p_items_6_project_statement_repartitioned_by_subject[ts_p_items_6_project_statement_repartitioned_by_subject]
PROJECT_S_OB_BY_PK_SINK(PROJECT_S_OB_BY_PK_SINK) --> ts_p_items_6_project_statement_with_ob_by_pk[ts_p_items_6_project_statement_with_ob_by_pk]
PROJECT_S_SUB_BY_PK_SINK(PROJECT_S_SUB_BY_PK_SINK) --> ts_p_items_6_project_statement_with_sub_by_pk[ts_p_items_6_project_statement_with_sub_by_pk]
PUBLIC_COMMUNITY_EDGE_SINK(PUBLIC_COMMUNITY_EDGE_SINK) --> ts_p_items_6_public_community_edges[ts_p_items_6_public_community_edges]
PUBLIC_COMMUNITY_ENTITY_SINK(PUBLIC_COMMUNITY_ENTITY_SINK) --> ts_p_items_6_public_community_entities[ts_p_items_6_public_community_entities]
PUBLIC_PROJECT_EDGE_SINK(PUBLIC_PROJECT_EDGE_SINK) --> ts_p_items_6_public_project_edges[ts_p_items_6_public_project_edges]
PUBLIC_PROJECT_ENTITY_SINK(PUBLIC_PROJECT_ENTITY_SINK) --> ts_p_items_6_public_project_entities[ts_p_items_6_public_project_entities]
TOOLBOX_COMMUNITY_EDGE_SINK(TOOLBOX_COMMUNITY_EDGE_SINK) --> ts_p_items_6_toolbox_community_edges[ts_p_items_6_toolbox_community_edges]
TOOLBOX_COMMUNITY_ENTITY_SINK(TOOLBOX_COMMUNITY_ENTITY_SINK) --> ts_p_items_6_toolbox_community_entities[ts_p_items_6_toolbox_community_entities]
TOOLBOX_PROJECT_EDGE_SINK(TOOLBOX_PROJECT_EDGE_SINK) --> ts_p_items_6_toolbox_project_edges[ts_p_items_6_toolbox_project_edges]
TOOLBOX_PROJECT_ENTITY_SINK(TOOLBOX_PROJECT_ENTITY_SINK) --> ts_p_items_6_toolbox_project_entities[ts_p_items_6_toolbox_project_entities]
subgraph Sub-Topology: 0
ts.information.resource-source(ts.information.resource-<br>source) --> REPARTITION_E_BY_PK(REPARTITION_E_BY_PK)
REPARTITION_E_BY_PK(REPARTITION_E_BY_PK) --> REPARTITIONED_E_BY_PK_SINK(REPARTITIONED_E_BY_PK_SINK)
end
subgraph Sub-Topology: 1
ts.projects.info_proj_rel-source(ts.projects.info_proj_rel-<br>source) --> REPARTITION_IPR_BY_FKE(REPARTITION_IPR_BY_FKE)
REPARTITION_IPR_BY_FKE(REPARTITION_IPR_BY_FKE) --> REPARTITIONED_IPR_BY_FKE_SINK(REPARTITIONED_IPR_BY_FKE_SINK)
end
subgraph Sub-Topology: 2
ts_statement_object_statement_with_literal-source(ts_statement_object_statement_with_literal-<br>source) --> REPARTITION_S_BY_PK(REPARTITION_S_BY_PK)
REPARTITION_S_BY_PK(REPARTITION_S_BY_PK) --> REPARTITIONED_S_BY_PK_SINK(REPARTITIONED_S_BY_PK_SINK)
end
subgraph Sub-Topology: 3
PROJECT_ENTITY_SOURCE(PROJECT_ENTITY_SOURCE) --> STOCK_PE(STOCK_PE)
REPARTITIONED_IPR_BY_FKE_SOURCE(REPARTITIONED_IPR_BY_FKE_SOURCE) --> JOIN_IPR(JOIN_IPR)
STOCK_PE(STOCK_PE) --> JOIN_PE_S_SUB(JOIN_PE_S_SUB)
STOCK_PE(STOCK_PE) --> JOIN_PE_S_OB(JOIN_PE_S_OB)
JOIN_IPR(JOIN_IPR) --> IPR_TO_S(IPR_TO_S)
JOIN_IPR(JOIN_IPR) --> IPR_TO_E(IPR_TO_E)
PROJECT_STATEMENT_REPARTITIONED_BY_SUB_SOURCE(PROJECT_STATEMENT_REPARTITIONED_BY_SUB_SOURCE) --> JOIN_S_SUB_PE(JOIN_S_SUB_PE)
JOIN_PE_S_SUB(JOIN_PE_S_SUB) --> CREATE_LITERAL_EDGES(CREATE_LITERAL_EDGES)
JOIN_PE_S_SUB(JOIN_PE_S_SUB) --> REPARTITION_PS_SUB_BY_PK(REPARTITION_PS_SUB_BY_PK)
JOIN_S_SUB_PE(JOIN_S_SUB_PE) --> CREATE_LITERAL_EDGES(CREATE_LITERAL_EDGES)
JOIN_S_SUB_PE(JOIN_S_SUB_PE) --> REPARTITION_PS_SUB_BY_PK(REPARTITION_PS_SUB_BY_PK)
PROJECT_S_OB_BY_PK_SOURCE(PROJECT_S_OB_BY_PK_SOURCE) --> JOIN_OB_WITH_SUB(JOIN_OB_WITH_SUB)
PROJECT_S_SUB_BY_PK_SOURCE(PROJECT_S_SUB_BY_PK_SOURCE) --> JOIN_SUB_WITH_OB(JOIN_SUB_WITH_OB)
CREATE_LITERAL_EDGES(CREATE_LITERAL_EDGES) --> FORK_EDGES(FORK_EDGES)
JOIN_OB_WITH_SUB(JOIN_OB_WITH_SUB) --> FORK_EDGES(FORK_EDGES)
JOIN_SUB_WITH_OB(JOIN_SUB_WITH_OB) --> FORK_EDGES(FORK_EDGES)
REPARTITIONED_E_BY_PK_SOURCE(REPARTITIONED_E_BY_PK_SOURCE) --> JOIN_E(JOIN_E)
FORK_EDGES(FORK_EDGES) --> TOOLBOX_CREATE_COMMUNITY_EDGES(TOOLBOX_CREATE_COMMUNITY_EDGES)
FORK_EDGES(FORK_EDGES) --> PUBLIC_CREATE_COMMUNITY_EDGES(PUBLIC_CREATE_COMMUNITY_EDGES)
FORK_EDGES(FORK_EDGES) --> PUBLIC_PROJECT_EDGE_SINK(PUBLIC_PROJECT_EDGE_SINK)
FORK_EDGES(FORK_EDGES) --> TOOLBOX_PROJECT_EDGE_SINK(TOOLBOX_PROJECT_EDGE_SINK)
IPR_TO_E(IPR_TO_E) --> FORK_ENTITIES(FORK_ENTITIES)
IPR_TO_E(IPR_TO_E) --> PROJECT_ENTITY_SINK(PROJECT_ENTITY_SINK)
JOIN_E(JOIN_E) --> FORK_ENTITIES(FORK_ENTITIES)
JOIN_E(JOIN_E) --> PROJECT_ENTITY_SINK(PROJECT_ENTITY_SINK)
REPARTITIONED_S_BY_PK_SOURCE(REPARTITIONED_S_BY_PK_SOURCE) --> JOIN_S(JOIN_S)
FORK_ENTITIES(FORK_ENTITIES) --> PUBLIC_COMMUNITY_ENTITY_SINK(PUBLIC_COMMUNITY_ENTITY_SINK)
FORK_ENTITIES(FORK_ENTITIES) --> PUBLIC_PROJECT_ENTITY_SINK(PUBLIC_PROJECT_ENTITY_SINK)
FORK_ENTITIES(FORK_ENTITIES) --> TOOLBOX_COMMUNITY_ENTITY_SINK(TOOLBOX_COMMUNITY_ENTITY_SINK)
FORK_ENTITIES(FORK_ENTITIES) --> TOOLBOX_PROJECT_ENTITY_SINK(TOOLBOX_PROJECT_ENTITY_SINK)
IPR_TO_S(IPR_TO_S) --> REPARTITION_S_BY_OBJECT(REPARTITION_S_BY_OBJECT)
IPR_TO_S(IPR_TO_S) --> REPARTITION_S_BY_SUBJECT(REPARTITION_S_BY_SUBJECT)
JOIN_S(JOIN_S) --> REPARTITION_S_BY_OBJECT(REPARTITION_S_BY_OBJECT)
JOIN_S(JOIN_S) --> REPARTITION_S_BY_SUBJECT(REPARTITION_S_BY_SUBJECT)
PROJECT_STATEMENT_REPARTITIONED_BY_OB_SOURCE(PROJECT_STATEMENT_REPARTITIONED_BY_OB_SOURCE) --> JOIN_S_OB_PE(JOIN_S_OB_PE)
JOIN_PE_S_OB(JOIN_PE_S_OB) --> PROJECT_S_OB_BY_PK_SINK(PROJECT_S_OB_BY_PK_SINK)
JOIN_S_OB_PE(JOIN_S_OB_PE) --> PROJECT_S_OB_BY_PK_SINK(PROJECT_S_OB_BY_PK_SINK)
PUBLIC_CREATE_COMMUNITY_EDGES(PUBLIC_CREATE_COMMUNITY_EDGES) --> PUBLIC_COMMUNITY_EDGE_SINK(PUBLIC_COMMUNITY_EDGE_SINK)
REPARTITION_PS_SUB_BY_PK(REPARTITION_PS_SUB_BY_PK) --> PROJECT_S_SUB_BY_PK_SINK(PROJECT_S_SUB_BY_PK_SINK)
REPARTITION_S_BY_OBJECT(REPARTITION_S_BY_OBJECT) --> PROJECT_STATEMENT_REPARTITIONED_BY_OB_SINK(PROJECT_STATEMENT_REPARTITIONED_BY_OB_SINK)
REPARTITION_S_BY_SUBJECT(REPARTITION_S_BY_SUBJECT) --> PROJECT_STATEMENT_REPARTITIONED_BY_SUB_SINK(PROJECT_STATEMENT_REPARTITIONED_BY_SUB_SINK)
TOOLBOX_CREATE_COMMUNITY_EDGES(TOOLBOX_CREATE_COMMUNITY_EDGES) --> TOOLBOX_COMMUNITY_EDGE_SINK(TOOLBOX_COMMUNITY_EDGE_SINK)
end
ts.information.resource
ts.projects.info_proj_rel
ts_statement_object_statement_with_entity
ts_statement_object_statement_with_literal
ts_p_items_6_project_entity
ts_p_items_6_ipr_repartitioned
ts_p_items_6_project_statement_repartitioned_by_subject
ts_p_items_6_project_statement_with_ob_by_pk
ts_p_items_6_project_statement_with_sub_by_pk
ts_p_items_6_e_repartitioned
ts_p_items_6_s_repartitioned
ts_p_items_6_project_statement_repartitioned_by_object
ts_p_items_6_e_repartitioned
ts_p_items_6_ipr_repartitioned
ts_p_items_6_s_repartitioned
ts_p_items_6_project_entity
ts_p_items_6_project_statement_repartitioned_by_object
ts_p_items_6_project_statement_repartitioned_by_subject
ts_p_items_6_project_statement_with_ob_by_pk
ts_p_items_6_project_statement_with_sub_by_pk
ts_p_items_6_public_community_edges
ts_p_items_6_public_community_entities
ts_p_items_6_public_project_edges
ts_p_items_6_public_project_entities
ts_p_items_6_toolbox_community_edges
ts_p_items_6_toolbox_community_entities
ts_p_items_6_toolbox_project_edges
ts_p_items_6_toolbox_project_entities
pe-store
e-store
s-store
ipr-store
s-sub-store
pe-store
s-sub-store
s-complete-store
s-ob-store
s-complete-store
entity-bool-store
edge-visibility-store
e-store
ipr-store
entity-bool-store
entity-count-store
s-store
ipr-store
s-ob-store
pe-store
s-ob-store
edge-sum-store
edge-count-store
edge-bool-store
edge-sum-store
edge-count-store
edge-bool-store
```