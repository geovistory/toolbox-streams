# Topology: CommunityClassLabel

This topology generates community labels of properties.


```mermaid
flowchart TD
    1d-->2a-->2b-->4a
    1c-->3c-->3a-->3b-->4a
    4a-->4b-->4c
    
    
    subgraph 1
        1c[geov_property_label]
        1d[ontome_property]
    end
    subgraph __2
        2a([FlatMap])
        2b([ToTable])
    end
    subgraph __3
        3c([filter])
        3a([SelectKey])
        3b([ToTable])
    end
    subgraph __4
        4a([Outer Join])
        4b([To])
        4c[community_property_label]
    end  
   
```

| Step |                                                                                                    |
|------|----------------------------------------------------------------------------------------------------|
| 1    | input topics                                                                                       |
| 2    | FlatMap: per input create two outputs of CommunityPropertyLabelKey and CommunityPropertyLabelValue |
| 3    | Filter default project and Co-partition by CommunityPropertyLabelKey                               |
| 4    | Join on OntomePropertyLabelKey taking geov label if present, else ontome label                     |
| 3    | To topic `community_property_label`                                                                |

## Input Topics

_{prefix_out} = TS_OUTPUT_TOPIC_NAME_PREFIX_

| name                                  | label in diagram      | Type   |
|---------------------------------------|-----------------------|--------|
| {output_prefix}_ontome_property_label | ontome_property_label | KTable |
| {output_prefix}_geov_property_label   | geov_property_label   | KTable |

## Output topic

| name                                     | label in diagram         |
|------------------------------------------|--------------------------|
| {output_prefix}_community_property_label | community_property_label |

## Output model

### key CommunityPropertyLabelKey

| field       | type    |
|-------------|---------|
| class_id    | int     |
| property_id | int     |
| is_outgoing | boolean |
| language_id | int     |

### value CommunityPropertyLabelValue

| field     | type          |
|-----------|---------------|
| label     | string        |
| __deleted | boolean, null |