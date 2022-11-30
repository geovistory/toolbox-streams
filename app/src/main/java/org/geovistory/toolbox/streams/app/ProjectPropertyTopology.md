# Topology: ProjectProperty

```mermaid
flowchart TD
    1a-->3a
    1b-->3a
    3a-->3b
    subgraph 1
        direction RL
        1a[project_profile]
        1b[api_property]
    end
    subgraph __3
        3a([Join])
        3b[project_property]    
    end
    

```

| Step |                                                                            |
|------|----------------------------------------------------------------------------|
| 1    | input topics                                                               |
| 3    | Join: on project, Key: projectId, domainId, propertyId, rangeId, Val: true |

## Input Topics

_{ns}= dev / stag / prod_

| name                               | label in diagram | Type   |
|------------------------------------|------------------|--------|
| {ns}.data_for_history.api_property | api_property     | KTable |
| {ns}.ts.project_profile            | project_profile  | KTable |

## Output topic

| name                     | label in diagram |
|--------------------------|------------------|
| {ns}.ts.project_property | project_property |

## Output model

| name  | description                                         |
|-------|-----------------------------------------------------|
| Key   | projectId, domainId, propertyId, rangeId            |
| Value | projectId, domainId, propertyId, rangeId, __deleted |
