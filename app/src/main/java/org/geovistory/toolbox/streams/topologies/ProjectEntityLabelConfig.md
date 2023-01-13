# Topology: ProjectEntityLabelConfig

This topology generates project entities by projectId and entityId.

```mermaid
flowchart TD
  
    2a-->3a
    1a-->2a
    1b-->2a
    1c-->3a-->3b

    subgraph 1
        1a[project_entity_label_config]
        1b[project_class]
        1c[community_entity_label_config]
    end
    subgraph __2
        2a([LeftJoin])
    end  
    subgraph __3
        3a([LeftJoin])
        3b[project_entity_label_config_enriched]
    end  
```

| Step |                                                                           |
|------|---------------------------------------------------------------------------|
| 1    | input topics                                                              |
| 2    | LeftJoin on class_id and project_id                                       |
| 3    | LeftJoin on class_id, if no project_entity_label_config was joined before |

## Input Topics

_{prefix_in} = TS_INPUT_TOPIC_NAME_PREFIX_

_{prefix_out} = TS_OUTPUT_TOPIC_NAME_PREFIX_

| name                                       | label in diagram            | Type   |
|--------------------------------------------|-----------------------------|--------|
| {input_prefix}_project_entity_label_config | project_entity_label_config | KTable |

## Output topics

| name                                                 | label in diagram                     |
|------------------------------------------------------|--------------------------------------|
| {output_prefix}_project_entity_label_config_enriched | project_entity_label_config_enriched |
| {output_prefix}_community_entity_label_config        | community_entity_label_config        |


## Output model project_entity_label_config_enriched

### key

| field      | type |
|------------|------|
| project_id | int  |
| class_id   | int  |

### value

| field     | type                   |
|-----------|------------------------|
| class_id  | int                    |
| config    | EntityLabelConfigValue |
| __deleted | boolean, null          |
