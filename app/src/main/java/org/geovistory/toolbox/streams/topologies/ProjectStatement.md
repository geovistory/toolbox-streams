# Topology: ProjectStatement

This topology generates project entities by projectId and statementId.

```mermaid
flowchart TD
    1b-->2a
    1a-->2a
    1c-->3a
    2a-->3a
    3a-->4a
    1c-->4a
    4a-->4b-->4c
   
    subgraph 1
        1a[statement_enriched]
        1b[info_proj_rel]
        1c[project_entity_label]
    end
    subgraph __2
        2a([Join])
    end  
        subgraph __3
        3a([LeftJoin])
    end
    subgraph __4
        4a([LeftJoin])
        4b([To])
        4c[project_statement]
        4c[project_statement]
    end  

```

| Step |                                                                       |
|------|-----------------------------------------------------------------------|
| 1    | input topics                                                          |
| 2    | Join on foreign key info_proj_rel.fk_entity = project_statement.id    |
| 3    | LeftJoin the entity label on the subject_id, adding the subject label |
| 4    | LeftJoin the entity label on the object_id, adding the object label   |
|      | To topic `project_statement`                                          |

## Input Topics

_{prefix_in} = TS_INPUT_TOPIC_NAME_PREFIX_

_{prefix_out} = TS_OUTPUT_TOPIC_NAME_PREFIX_

| name                                  | label in diagram   | Type   |
|---------------------------------------|--------------------|--------|
| {input_prefix}_projects_info_proj_rel | info_proj_rel      | KTable |
| {prefix_out}_statement_enriched       | statement_enriched | KTable |

## Output topic

| name                              | label in diagram  |
|-----------------------------------|-------------------|
| {output_prefix}_project_statement | project_statement |

## Output model

| name  | description                                  |
|-------|----------------------------------------------|
| Key   | projectId, statementId                       |
| Value | projectId, statementId, Statement, __deleted |

### Key

| field       | type |
|-------------|------|
| projectId   | int  |
| statementId | int  |

### Value

| field         | type                            |
|---------------|---------------------------------|
| projectId     | int                             |
| statementId   | int                             |
| statement     | StatementEnrichedValue          |
| created_by    | int                             |
| created_at    | io.debezium.time.ZonedTimestamp |
| modified_at   | io.debezium.time.ZonedTimestamp |
| is_in_project | boolean                         |
| __deleted     | boolean, null                   |
