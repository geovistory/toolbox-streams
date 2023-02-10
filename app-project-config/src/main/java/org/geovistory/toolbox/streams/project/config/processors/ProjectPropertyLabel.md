# Topology: ProjectPropertyLabel

This topology generates project labels of properties.

For each project-property, the first label present in the following list is used:

- Label in project language, provided by Geovistory project
- Label in project language, provided by Geovistory default project
- Label in project language, provided by OntoME
- Label in english, provided by Geovistory project
- Label in english, provided by Geovistory default project
- Label in english, provided by OntoME

To achieve this, the topology joins `project_property` with `project`, `text_property` and `api_property`.

- `project_property`: For each record in this topic two labels are generated: the outgoing and the ingoing label.
- `project` Provides the language of a project.
- `text_property` provides the Geovistory (default) project labels
- `api_property` provides the OntoME labels

```mermaid
flowchart TD
    1a-->2a
    1b-->2a
    2a-->3a
    1c-->4.1a
    1c-->4.2a
    4.1a-->4.2a-->5a
    3a-->3b
    3b-->4.1a
    1d-->5a
    5a-->6a-->7a-->8a-->8b
    subgraph 1
        1a[project_property]
        1b[project]
        1c[geov_property_label]
        1d[ontome_property_label]
    end
    subgraph __2
        2a([Join])
    end  
    subgraph __3
        3a([Flat Map])
        3b([toTable])
    end  
    subgraph __4.1 out project
        4.1a([Left Join])
    end  
    subgraph __4.2 out default
        4.2a([Left Join])
    end  

    subgraph __5
        5a([Left Join])
    end  
    subgraph __6
        6a([Group By])
    end  
    subgraph __7
        7a([Aggregate])
    end  
    subgraph __8
        8a([To])
        8b[project_property_label]
    end  
```

| Step |                                                                                                                          |
|------|--------------------------------------------------------------------------------------------------------------------------|
| 1    | input topics                                                                                                             |
| 2    | Join on `projectId = project.pk_entity`; enrich the value of `project_property` by `project.fk_language` as `languageId` |
| 3    | Flat Map: for each input record property add a record for the incoming and one for the outgoing field                    |                                                           
|      | and where `languageId` not en `18889` add a duplicate with `18889`                                                       |                                                           
|      | Keys: ProjectId, ClassId, PropertyId, IsOutgoing, LanguageId                                                             |                                                           
| 4.1  | Left Join label `geov_property_label` of the project                                                                     |
| 4.2  | Left Join label  `geov_property_label` of the default project 375669                                                     |
| 5    | Left Join on keys                                                                                                        |
| 6    | Group By  ProjectId, PropertyId                                                                                          |
| 7    | Aggregate picking the label as defined above                                                                             |
| 8    | To topic `project_property_label`                                                                                        |

## Input Topics

_{prefix_in} = TS_INPUT_TOPIC_NAME_PREFIX_

_{prefix_out} = TS_OUTPUT_TOPIC_NAME_PREFIX_

| name                                  | label in diagram      | Type   |
|---------------------------------------|-----------------------|--------|
| {output_prefix}_ontome_property_label | ontome_property_label | KTable |
| {output_prefix}_geov_property_label   | geov_property_label   | KTable |
| {input_prefix}_projects_project       | project               | KTable |
| {output_prefix}_project_property      | project_property      | KTable |

## Output topic

| name                                   | label in diagram       |
|----------------------------------------|------------------------|
| {output_prefix}_project_property_label | project_property_label |

## Output model

### Key

| field      | type    |
|------------|---------|
| projectId  | int     |
| classId    | int     |
| propertyId | int     |
| isOutgoing | boolean |

### Value

| field      | type          |
|------------|---------------|
| projectId  | int           |
| classId    | int           |
| propertyId | int           |
| isOutgoing | boolean       |
| languageId | int           |
| label      | int           |
| deleted    | boolean, null |