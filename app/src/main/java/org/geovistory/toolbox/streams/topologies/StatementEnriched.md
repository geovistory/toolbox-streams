# Topology: Statement Enriched

This topology enriches statements with their literal values.

```mermaid
flowchart TD
    1a-->4a
    1b-->2b-->3a
    1c-->2c-->3a-->3b
    1d-->2d-->3b-->3c 
    1e-->2e-->3c-->3d 
    1f-->2f-->3d-->3e 
    1g-->2g-->3e-->4a-->4b-->4c-->4d
       
    subgraph 1
        1a[statement]
        1b[language]
        1c[appellation]
        1d[lang_string]
        1e[place]
        1f[timePrimitive]
        1g[dimension]
    end
    subgraph __2
        2b([2b MapValues])
        2c([2c MapValues])
        2d([2d MapValues])
        2e([2e MapValues])
        2f([2f MapValues])
        2g([2g MapValues])
       
    end  
    subgraph __3
        3a([3a Merge])
        3b([3b Merge])
        3c([3c Merge])
        3d([3d Merge])
        3e([3e Merge])
    end  
    subgraph __4
        4a([Left Join])
        4b([ToStream])
        4c([To])
        4d[statements_enriched]
    end  
    
```


| Step |                                                              |
|------|--------------------------------------------------------------|
| 1    | input topics                                                 |
| 2    | MapValues to  StatementObject                                |
| 3    | merge streams enriching StatementObject                      |
| 4    | Left join statement objects with statement on fk_object_info |

class StatementObject

| property | type                 |
|----------|----------------------|
| classId  | Integer              |
| label    | String               |
| value    | StatementObjectValue |

class StatementObjectValue

| property      | type                        |
|---------------|-----------------------------|
| language      | null, InfLanguageValue      |
| appellation   | null, InfAppellationValue   |
| langString    | null, InfLangStringValue    |
| place         | null, InfPlaceValue         |
| timePrimitive | null, InfTimePrimitiveValue |
| dimension     | null, InfDimension          |

## Input Topics

_{prefix_in} = TS_INPUT_TOPIC_NAME_PREFIX_

_{prefix_out} = TS_OUTPUT_TOPIC_NAME_PREFIX_

| name                                  | label in diagram | Type   |
|---------------------------------------|------------------|--------|
| {input_prefix}_projects_info_proj_rel | info_proj_rel    | KTable |
| {input_prefix}_information_resource   | resource         | KTable |

## Output topic

| name                               | label in diagram   |
|------------------------------------|--------------------|
| {output_prefix}_statement_enriched | statement_enriched |

## Output model

### Key

| field      | type |
|------------|------|
| subjectId  | int  |
| propertyId | int  |
| objectId   | int  |

### Value

| field       | type            |
|-------------|-----------------|
| subjectId   | int             |
| propertyId  | int             |
| objectId    | int             |
| objectValue | StatementObject |
| deleted     | boolean, null   |

