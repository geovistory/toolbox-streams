# Topology: ProjectEntityTimeSpan

This topology aggregates entity time span.

```mermaid 
flowchart TD  
    1a-->2a-->2b  
    subgraph 1  
        1a[project_entity_top_statements]  
    end  
    subgraph __2  
        2a([flatMapValues])  
        2b[project_entity_time_span]  
    end  

```

| Step |                                     |
|------|-------------------------------------|
| 1    | input topic                         |
| 2    | flatMap                             |
|      | To topic `project_entity_time_span` |

## Input Topics

_{prefix_in} = TS_INPUT_TOPIC_NAME_PREFIX_

_{prefix_out} = TS_OUTPUT_TOPIC_NAME_PREFIX_

| name                                       | label in diagram              | Type    |
|--------------------------------------------|-------------------------------|---------|
| {prefix_out}_project_entity_top_statements | project_entity_top_statements | KStream |

## Output topic

| name                                  | label in diagram         |
|---------------------------------------|--------------------------|
| {prefix_out}_project_entity_time_span | project_entity_time_span |

## Output model

### Key

| field      | type   |
|------------|--------|
| project_id | int    |
| entity_id  | string |

### Value

| field       | type          |
|-------------|---------------|
| timeSpan    | TimeSpan      |
| firstSecond | long          |
| lastSecond  | long          |
| __deleted   | boolean, null |

### TimeSpan

| field | type          |
|-------|---------------|
| p81   | TimePrimitive |
| p82   | TimePrimitive |
| p81a  | TimePrimitive |
| p81b  | TimePrimitive |
| p82a  | TimePrimitive |
| p82b  | TimePrimitive |

### TimePrimitive

| field | type |
| --- | --- |
| julianDay | long |
| duration | string |
| calendar | string |