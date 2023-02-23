# Topology: ProjectProperty

```mermaid
flowchart TD
    1b-->2a-->3a-->3b-->5a
    1a-->4a
    4a-->5a-->6a-->6b-->6c-->6d
    subgraph 1
        direction RL
        1a[project_profile]
        1b[api_property]
    end
    subgraph __2
        2a([MapValues])
    end
    subgraph __3
        3a([GroupBy])
        3b([Aggregate])
    end
    subgraph __4
        4a([ToTable])
    end
    subgraph __5
        5a([Join])
    end
       subgraph __6
        6a([ToStream])
        6b([FlatMap])
        6c[To]
        6d[project_property]    
    end
    

```

| Step |                                                                                                                                                                                                             |
|------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1    | input topics                                                                                                                                                                                                |
| 2    | MapValues: project the stream to ProfilePropertyValue: profileId, domainId, propertyId, rangeId, deleted                                                                                                    |
| 3    | GroupBy profileId and Aggregate to a PropertyProfileMap, concatenating profileId, domainId, propertyId and rangeId to a key and using ProfilePropertyValue as value. This removes duplicates of properties. |
| 4    | ToTable                                                                                                                                                                                                     |
| 5    | Join: KTable-KTable-Foreign-key-Join on profileId                                                                                                                                                           |
| 6    | ToStream: convert table to a stream; FlatMap: Flatten aggregated properties to project-property messages; To: Write to topic project_property                                                               |

## Input Topics

_{prefix_in} = TS_INPUT_TOPIC_NAME_PREFIX_
 
_{prefix_out} = TS_OUTPUT_TOPIC_NAME_PREFIX_

| name                                      | label in diagram | Type    |
|-------------------------------------------|------------------|---------|
| {prefix_in}.data_for_history.api_property | api_property     | KTable  |
| {prefix_out}.project_profile              | project_profile  | KStream |

## Output topic

| name                      | label in diagram |
|---------------------------|------------------|
| {prefix}.project_property | project_property |

## Output model

| name  | description                                         |
|-------|-----------------------------------------------------|
| Key   | projectId, domainId, propertyId, rangeId            |
| Value | projectId, domainId, propertyId, rangeId, __deleted |
