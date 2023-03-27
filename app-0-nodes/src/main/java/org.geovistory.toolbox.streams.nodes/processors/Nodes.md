# Topology: Statement Enriched

This topology enriches statements with their objects.

```mermaid
flowchart TD
    1b-->2b-->3a
    1c-->2c-->3a-->3b
    1d-->2d-->3b-->3c 
    1e-->2e-->3c-->3d 
    1f-->2f-->3d-->3e 
    1g-->2g-->3e-->3f
    1h-->2h-->3f-->3g 
    1i-->2i-->3g-->3h
    1j-->2j-->3h-->4c-->4d
    4c-->4e
    subgraph 1
        1b[language]
        1c[appellation]
        1d[lang_string]
        1e[place]
        1f[timePrimitive]
        1g[dimension]
        1h[table]
        1i[cell]
        1j[resource]
    end
    subgraph __2
        2b([2b MapValues])
        2c([2c MapValues])
        2d([2d MapValues])
        2e([2e MapValues])
        2f([2f MapValues])
        2g([2g MapValues])
        2h([2h MapValues])
        2i([2i MapValues])
        2j([2j MapValues])
       
    end  
    subgraph __3
        3a([3a Merge])
        3b([3b Merge])
        3c([3c Merge])
        3d([3d Merge])
        3e([3e Merge])
        3f([3f Merge])
        3g([3g Merge])
        3h([3h Merge])
    end  
    subgraph __4
        4c([To])
        4d[nodes_1]
        4e[nodes_2]
    end  
    
```

| Step |                              |
|------|------------------------------|
| 1    | input topics                 |
| 2    | MapValues to Node            |
| 3    | merge streams enriching Node |
| 4    | sink to two topics           |

## Input Topics

_{prefix_in} = TS_INPUT_TOPIC_NAME_PREFIX_

_{prefix_out} = TS_OUTPUT_TOPIC_NAME_PREFIX_

| name                                      | label in diagram | Type    |
|-------------------------------------------|------------------|---------|
| {input_prefix}_information_resource       | resource         | KStream |
| {input_prefix}_information_language       | language         | KStream |
| {input_prefix}_information_appellation    | appellation      | KStream |
| {input_prefix}_information_lang_string    | lang_string      | KStream |
| {input_prefix}_information_place          | place            | KStream |
| {input_prefix}_information_time_primitive | time_primitive   | KStream |
| {input_prefix}_information_dimension      | dimension        | KStream |
| {input_prefix}_information_table          | table            | KStream |
| {input_prefix}_information_cell           | cell             | KStream |

## Output topics

| name                  | label in diagram |
|-----------------------|----------------|
| {output_prefix}_nodes | nodes          |

## Output model

### Key

NodeKey

### Value

NodeValue