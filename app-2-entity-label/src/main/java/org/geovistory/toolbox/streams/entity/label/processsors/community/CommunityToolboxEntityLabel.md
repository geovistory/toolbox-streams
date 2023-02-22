# Topology: CommunityToolboxEntityLabel

This topology generates entity labels by entityId.

```mermaid
flowchart TD
    1b-->2a
    1a-->2a-->3a-->4a
    1c-->4a-->5a-->6a-->6b
   
    subgraph 1
        1a[community_toolbox_entity]
        1b[community_entity_label_config_enriched]
        1c[community_toolbox_top_statements]
    end
    subgraph __2
        2a([LeftJoin])
    end  
    subgraph __3
        3a([FlatMap])
    end  
    subgraph __4
        4a([LeftJoin])
    end  
    subgraph __5
        5a([GroupBy])
    end  
        subgraph __6
        6a([Aggregate])
        6b[community_toolbox_entity_label]
    end  
```

| Step |                                                                                                                     |
|------|---------------------------------------------------------------------------------------------------------------------|
| 1    | input topics                                                                                                        |
| 2    | LeftJoin community_toolbox_entity with community_entity_label_config_enriched `class_id` and `community_toolbox_id` |
| 3    | FlatMap create a record for each item in the array of entity label config                                           |
| 4    | LeftJoin on entity_id, community_toolbox_id, property_id, is_outgoing                                               |
| 5    | Group By entity_id, community_toolbox_id                                                                            |
| 6    | Aggregate the label to a comma separate string with max length of 100 characters                                    |

## Input Topics

_{prefix_in} = TS_INPUT_TOPIC_NAME_PREFIX_

_{prefix_out} = TS_OUTPUT_TOPIC_NAME_PREFIX_

| name                                                | label in diagram                       | Type   |
|-----------------------------------------------------|----------------------------------------|--------|
| {prefix_out}_community_entity_label_config_enriched | community_entity_label_config_enriched | KTable |
| {prefix_out}_community_toolbox_entity               | community_toolbox_entity               | KTable |
| {prefix_out}_community_toolbox_top_statements       | community_toolbox_top_statements       | KTable |

## Output topic

| name                                           | label in diagram               |
|------------------------------------------------|--------------------------------|
| {output_prefix}_community_toolbox_entity_label | community_toolbox_entity_label |

## Output model

### Key CommunityToolboxEntityKey

| field                | type   |
|----------------------|--------|
| entity_id            | string |

### Value CommunityToolboxEntityLabelValue

| field                | type          |
|----------------------|---------------|
| entity_id            | string        |
| label                | string        |
| __deleted            | boolean, null |
