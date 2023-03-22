# Topology: ProjectEntityFulltext

This topology generates community entities full text by communityId and entityId.

```mermaid
flowchart TD
    1b-->5a
    1c-->3a-->4a
    1a-->2a-->3a
    4a-->4b-->5a-->6a-->6b
   
    subgraph 1
        1a[community_top_statements]
        1b[community_entity_with_label_config-changelog]
        1c[community_property_label]
    end
    subgraph __2
        2a([MapValues])
    end
    subgraph __3
        3a([LeftJoin])
    end
    subgraph __4
        4a([GroupBy])
        4b([Aggregate])
    end
    subgraph __5
        5a([LeftJoin])
    end  
    subgraph __6
        6a([MapValues])
        6b[community_entity_full_text]
    end  
```

| Step |                                                 |
|------|-------------------------------------------------|
| 1    | input topics                                    |
| 2    | MapValues: Convert top statements to top labels |
| 3    | LeftJoin PropertyLabel                          |
| 4    | GroupBy ProjectEntityKey                        |
| 4    | Aggregate EntityFulltextsMap                    |
| 5    | LeftJoin community_entity_label_config_enriched |
| 6    | MapValues Create fulltext                       |

## Input Topics

_{prefix_in} = TS_INPUT_TOPIC_NAME_PREFIX_

_{prefix_out} = TS_OUTPUT_TOPIC_NAME_PREFIX_

| name                                                | label in diagram                       | Type   |
|-----------------------------------------------------|----------------------------------------|--------|
| {prefix_out}_community_entity_label_config_enriched | community_entity_label_config_enriched | KTable |
| {prefix_out}_community_entity_top_statements        | community_entity_top_statements        | KTable |

## Output topic

| name                                       | label in diagram           |
|--------------------------------------------|----------------------------|
| {output_prefix}_community_entity_full_text | community_entity_full_text |

## Output model

### Key

| field        | type   |
|--------------|--------|
| entity_id    | string |

### Value

| field        | type          |
|--------------|---------------|
| entity_id    | string        |
| full_text    | string        |
| __deleted    | boolean, null |
