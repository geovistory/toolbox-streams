# Topology: ProjectStatementToUri

```mermaid
flowchart TD
    1a-->2a-->3a-->3b

    
    
    subgraph 1
        direction RL
        1a([project_statement_with_entity])
       
    end
    subgraph __2
        2a([FlatMap])
    end       
    subgraph __3
        3a([To])
        3b[project_rdf]
    end
    
```

| Step |                                                                                                                  |
|------|------------------------------------------------------------------------------------------------------------------|
| 1    | input topics                                                                                                     |
| 2    | FlatMap each input ProjectStatementKey-ProjectStatementValue-record to two ProjectRdfKey-ProjectRdfValue-records |
|      | one with subject, predicate, object and the inversed one with object, predicate+i, subject                       |
| 3    | To output topic project_rdf                                                                                      |

## Input Topics

_{prefix_out} = TS_OUTPUT_TOPIC_NAME_PREFIX_

| name                                       | label in diagram              | Type    |
|--------------------------------------------|-------------------------------|---------|
| {prefix_out}_project_statement_with_entity | project_statement_with_entity | KStream |

## Output topic

| name                        | label in diagram |
|-----------------------------|-----------------|
| {output_prefix}_project_rdf | project_rdf     |

## Output model

### Key: ProjectRdfKey

| field      | type   |
|------------|--------|
| project_id | int    |
| turtle     | string |

### Value: ProjectRdfValue

| field        | type                    |
|--------------|-------------------------|
| operation    | enum["insert","delete"] |