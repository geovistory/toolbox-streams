# Topology: Statement Enriched

This topology enriches statements with their objects.

```mermaid
flowchart TD
    1a-->5a
    1b-->5a-->5b-->5c-->5d
    5c-->5e
    subgraph 1
        1a[statement_with_subject]
        1b[node]
    end
    subgraph __5
        5a([Join])
        5b([ToStream])
        5c([To])
        5d[statement_enriched_with_entity]
        5e[statement_enriched_with_literal]
    end  
    
```

| Step |                                                                                   |
|------|-----------------------------------------------------------------------------------|
| 1    | input topics                                                                      |
| 5    | Join statement with Nodes on object id                                            |
|      | Split stream in branches of statements with literals and statements with entities |

## Input Topics

_{prefix_out} = TS_OUTPUT_TOPIC_NAME_PREFIX_

| name                                | label in diagram       | Type   |
|-------------------------------------|------------------------|--------|
| {prefix_out}_statement_with_subject | statement_with_subject | KTable |
| {prefix_out}_node                   | node                   | KTable |

## Output topics

| name                                            | label in diagram                |
|-------------------------------------------------|---------------------------------|
| {output_prefix}_statement_enriched_with_entity  | statement_enriched_with_entity  |
| {output_prefix}_statement_enriched_with_literal | statement_enriched_with_literal |

## Output model

### Key

InfStatementKey

### Value

StatementEnrichedValue