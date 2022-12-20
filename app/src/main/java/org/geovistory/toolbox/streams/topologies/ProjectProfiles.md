# Topology: ProjectProfiles

```mermaid
flowchart TD
    1a-->7a
    7a-->8a
    8a-->9a
    1c-->2a
    2a-->3a
    3a-->4a
    1b-->5a
    5a-->6a
    4a-->10a
    6a-->9a
    9a-->10a
    10a-->11a
    11a-->12a
    12a-->13a
    13a-->14a
    14a-->14b
    subgraph 1
        direction RL
        1a[project]
        1b[config]
        1c[dfh_profile_proj_rel]
    end
    subgraph __2
       2a([Filter])
    end
    subgraph __3
        3a([Group])
    end
    subgraph __4
        4a([Aggregate])
    end

    subgraph __5
       5a([Filter])
    end
    subgraph __6
       6a([Map])
    end
    subgraph __7
       7a([Map])
    end
    subgraph __8
       8a([ToTable])
    end
    subgraph __9
       9a([LeftJoin])
    end
    subgraph ____10
       10a([LeftJoin])
    end
    subgraph ____11
       11a([MapValues])
    end
    subgraph ____12
       12a([GroupBy])
    end
    subgraph ____13
       13a([Reduce])
    end
    subgraph ____14
       14a([FlatMap])
       14b[project_profiles]
    end
```

| Step |                                                                             |
|------|-----------------------------------------------------------------------------|
| 1    | input topics                                                                |
| 2    | Filter: only enabled profiles project relations                             |
| 3    | Group: by project                                                           |
| 4    | Aggregate: Key: project, Val: array of profiles                             |
| 5    | Filter: only rows with key = SYS_CONFIG                                     |
| 6    | Map: Key=constant Val=array of required profiles from sys config json       |
| 8    | ToTable: to table                                                           |
| 7    | Map: Key=project id, Value=project id (we only need project ids)            |
| 9    | LeftJoin: projects (left) with config (right) with required profiles        |
| 10   | LeftJoin: 10 (left) with 4 (right) to projects_with_aggregated_profiles     |
| 11   | Values: BooleanMap with profileId as key and false as value (= not deleted) |
| 12   | GroupByKey                                                                  |
| 13   | Reduce: mark missing profiles in new value as deleted                       |
| 14   | FlatMap: for each profile per project create one profileProject record      |

## Input Topics

_{ns}= dev / stag / prod_

| name                               | label in diagram     | Type   |
|------------------------------------|----------------------|--------|
| {ns}.projects.project              | project              | KTable |
| {ns}.projects.dfh_profile_proj_rel | dfh_profile_proj_rel | KTable |
| {ns}.system.config                 | config               | KTable |

## Output topic

| name                                      | label in diagram                  |
|-------------------------------------------|-----------------------------------|
| {ns}.ts.projects_with_aggregated_profiles | projects_with_aggregated_profiles |

## Output model

| name  | description                     |
|-------|---------------------------------|
| Key   | projectId, profileId            |
| Value | projectId, profileId, __deleted |
