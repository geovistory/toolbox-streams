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
    10a-->10b
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
    subgraph __9
       9a([LeftJoin])
    end
    subgraph 10
       10a([LeftJoin])
       10b[project_profiles]
    end
    subgraph __7
       7a([Map])
    end
    subgraph __8
       8a([ToTable])
    end

```

| Step |                                                                       |
|------|-----------------------------------------------------------------------|
| 1    | input topics                                                          |
| 2    | Filter: only enabled profiles project relations                       |
| 3    | Group: by project                                                     |
| 4    | Aggregate: Key: project, Val: array of profiles                       |
| 5    | Filter: only rows with key = SYS_CONFIG                               |
| 6    | Map: Key=constant Val=array of required profiles from sys config json |
| 8    | ToTable: to table                                                     |
| 7    | Map: Key=project id, Value=project id (we only need project ids)      |
| 9    | LeftJoin: projects (left) with config (right) with required profiles  |
| 10   | LeftJoin: 10 (left) with 4 (right) to project_profiles                |

## Input Topics

_{ns}= dev / stag / prod_

| name                               | label in diagram     | Type   |
|------------------------------------|----------------------|--------|
| {ns}.projects.project              | project              | KTable |
| {ns}.projects.dfh_profile_proj_rel | dfh_profile_proj_rel | KTable |
| {ns}.system.config                 | config               | KTable |

## Output topic

| name                     | label in diagram |
|--------------------------|------------------|
| {ns}.ts.project_profiles | project_profiles |

## Output model

| name  | description                                                           |
|-------|-----------------------------------------------------------------------|
| Key   | id of geovistory project                                              |
| Value | Array of OntoME profile ids enabled by project or required by system. |
