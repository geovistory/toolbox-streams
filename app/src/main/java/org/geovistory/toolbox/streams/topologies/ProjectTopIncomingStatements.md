# Topology: ProjectTopIncomingStatements

This topology aggregates the top 5 incoming statements of an entity's property.

```mermaid
flowchart TD
    1a-->2a-->3a-->3b
   
    subgraph 1
        1a[project_statement_enriched]
    end
    subgraph __2
        2a([GroupBy])
    end  
    subgraph __3
        3a([Aggregate])
        3b[project_top_incoming_statements]
    end

```

| Step |                                                                     |
|------|---------------------------------------------------------------------|
| 1    | input topic                                                         |
| 2    | GroupBy: group by subject_id                                        |
| 3    | Aggregate: create a list of statements, ordered by ord_num_of_range |
|      | To topic `project_top_incoming_statements`                          |

## Input Topics

_{prefix_in} = TS_INPUT_TOPIC_NAME_PREFIX_

_{prefix_out} = TS_OUTPUT_TOPIC_NAME_PREFIX_

| name                                    | label in diagram           | Type   |
|-----------------------------------------|----------------------------|--------|
| {prefix_out}_project_statement_enriched | project_statement_enriched | KTable |

## Output topic

| name                                         | label in diagram                |
|----------------------------------------------|---------------------------------|
| {prefix_out}_project_top_incoming_statements | project_top_incoming_statements |

## Output model

### Key

| field       | type    |
|-------------|---------|
| entity_id   | string  |
| project_id  | int     |
| property_id | int     |
| is_outgoing | boolean |

### Value

| field       | type                         |
|-------------|------------------------------|
| entity_id   | string                       |
| project_id  | int                          |
| property_id | int                          |
| is_outgoing | boolean                      |
| statements  | Array<ProjectStatementValue> |
| __deleted   | boolean, null                |
