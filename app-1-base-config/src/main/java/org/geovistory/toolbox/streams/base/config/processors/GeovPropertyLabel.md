# Topology: GeovPropertyLabel

This topology generates geovistory property labels, keyed by projectId, propertyId and languageId.

- `text_property` provides the Geovistory (default) project labels

```mermaid
flowchart TD

    1a-->2a-->2b-->2c
   
    subgraph 1
        1a[text_property]
    end
    subgraph __2
        2a([Flat Map])
        2b([To])
        2c[geov_property_label]
    end  
```

| Step |                                                                                                                                                                                                                                                                                             |
|------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1    | input topics                                                                                                                                                                                                                                                                                |
| 2    | Flat Map: keep only records with `fk_dfh_property` and `fk_system_type = 639`, <br/>Key: PropertyId=`fk_dfh_property`, ProjectId=`fk_pro_project`, LanguageId=`fk_language`, <br/>Value: PropertyId=`fk_dfh_property`, ProjectId=`fk_pro_project`, LanguageId=`fk_language`, Label=`string` |
|      | To topic `project_property_label`                                                                                                                                                                                                                                                           |

## Input Topics

_{prefix_in} = TS_INPUT_TOPIC_NAME_PREFIX_

_{prefix_out} = TS_OUTPUT_TOPIC_NAME_PREFIX_

| name                                  | label in diagram | Type   |
|---------------------------------------|------------------|--------|
| {input_prefix}_projects_text_property | text_property    | KTable |

## Output topic

| name                                | label in diagram    |
|-------------------------------------|---------------------|
| {output_prefix}_geov_property_label | geov_property_label |

## Output model

| name  | description                                         |
|-------|-----------------------------------------------------|
| Key   | projectId, propertyId, languageId                   |
| Value | projectId, propertyId, languageId, label, __deleted |
