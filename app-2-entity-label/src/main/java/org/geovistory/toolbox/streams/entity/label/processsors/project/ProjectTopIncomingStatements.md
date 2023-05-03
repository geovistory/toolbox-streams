# Topology: ProjectTopIncomingStatements

This topology aggregates the top 5 incoming statements of an entity's property.

```mermaid
flowchart TD
    1b-->3a
    1a-->2a
    1c-->2a-->3a-->4a-->5a-->5b 
    
    subgraph 1
        1a[project_statement_with_entity]
        1b[community_toolbox_entity_label]
        1c[project_entity_label]
    end
    subgraph __2
        2a([LeftJoin])
    end 
    subgraph __3
        3a([LeftJoin])
    end  
    subgraph __4
        4a([GroupBy])
    end  
    subgraph __5
        5a([Aggregate])
        5b[project_top_incoming_statements]
    end

```

| Step |                                                                     |
|------|---------------------------------------------------------------------|
| 1    | input topic                                                         |
| 2    | LeftJoin: project entity label to set subject label                 |
| 3    | LeftJoin: community entity label to set subject label               |
| 4    | GroupBy: group by subject_id                                        |
| 5    | Aggregate: create a list of statements, ordered by ord_num_of_range |
|      | To topic `project_top_incoming_statements`                          |

## Input Topics

_{prefix_in} = TS_INPUT_TOPIC_NAME_PREFIX_

_{prefix_out} = TS_OUTPUT_TOPIC_NAME_PREFIX_

| name                                       | label in diagram              | Type    |
|--------------------------------------------|-------------------------------|---------|
| {prefix_out}_project_statement_with_entity | project_statement_with_entity | KStream |

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

| field       | type                    |
|-------------|-------------------------|
| entity_id   | string                  |
| project_id  | int                     |
| property_id | int                     |
| is_outgoing | boolean                 |
| edges       | Array<ProjectEdgeValue> |
| __deleted   | boolean, null           |
