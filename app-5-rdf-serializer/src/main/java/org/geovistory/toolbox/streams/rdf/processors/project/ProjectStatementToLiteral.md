# Topology: ProjectStatementToLiteral

```mermaid
flowchart TD
    1a-->2a-->3a-->3b

    
    
    subgraph 1
        direction RL
        1a([project_statement_with_literal])
       
    end
    subgraph __2
        2a([Map])
    end       
    subgraph __3
        3a([To])
        3b[project_rdf]
    end
    
```

| Step |                                                                                                                  |
|------|------------------------------------------------------------------------------------------------------------------|
| 1    | input topics                                                                                                     |
| 2    | Map each input ProjectStatementKey-ProjectStatementValue-record to two ProjectRdfKey-ProjectRdfValue-records |
|      | one with subject, predicate, object and the inversed one with object, predicate+i, subject                       |
| 3    | To output topic project_rdf                                                                                      |

## Input Topics

_{prefix_out} = TS_OUTPUT_TOPIC_NAME_PREFIX_

| name                                        | label in diagram               | Type    |
|---------------------------------------------|--------------------------------|---------|
| {prefix_out}_project_statement_with_literal | project_statement_with_literal | KStream |

## Output topic

| name                        | label in diagram |
|-----------------------------|------------------|
| {output_prefix}_project_rdf | project_rdf      |

## Output model

### Key: ProjectRdfKey

| field      | type   |
|------------|--------|
| project_id | int    |
| turtle     | string |

### Value: ProjectRdfValue

| field     | type                    |
|-----------|-------------------------|
| operation | enum["insert","delete"] |

## RDF serialization

On the path `statement.object` of a ProjectStatementValue you find a NodeValue:

```json5
// ProjectStatementValue:
{
  // ...
  statement: {
    // java class StatementEnrichedValue
    // ...
    object: {
      // java class NodeValue 
      entity: null,
      // null or java class Entity 
      language: null,
      // null or java class Language 
      appellation: null,
      // null or java class Appellation
      lang_string: null,
      // null or java class LangString
      place: null,
      // null or java class Place
      time_primitive: null,
      // null or java class TimePrimitive
      dimension: null,
      // null or java class Dimension
      cell: null,
      // null or java class Cell
      digital: null
      // null or java class Digital
    }
  }
}
```

NodeValue has 9 keys; 8 are always null.

Depending on the not-null key the produced RDF (turtle) is different.

Statements with `entity` objects are converted to triples with URIs and are not covered by this spec (see
ProjectStatementToUri).

The other (8) object types are discussed below:

### Language

#### In

```json5
// ProjectStatementValue:
{
  'project_id': 567,
  'statement_id': 345,
}
```

```json5
// ProjectStatementValue:
{
  'project_id': 567,
  'subject_id': 'i1761647',
  'property_id': 1112,
  'object_id': 'i2255949',
  'statement': {
    'object': {
      'language': {
        // ...
        'notes': 'Italian',
        'pk_language': 'it'
      }
    }
  },
  '__deleted': 'false',
}
```

#### Out

```json5
// ProjectRdfKey
{
  'project_id': 567,
  'turtle': '<http://geovistory.org/resource/i1761647> <https://ontome.net/ontology/p1112> "Italian"^^<http://www.w3.org/2001/XMLSchema#string> .'
}
```

```json5
// ProjectRdfValue
{
  'operation': 'insert'
}
```

Remark: `language.notes` can be null. In this case pk_language has to be taken.

### Appellation

Remark: The term 'Appellation' is historically grown. Think of it as 'String'.

#### In

```json5
// ProjectStatementValue:
{
  'project_id': 567,
  'statement_id': 345,
}
```

```json5
// ProjectStatementValue:
{
  'project_id': 567,
  'subject_id': 'i1761647',
  'property_id': 1113,
  'object_id': 'i2255949',
  'statement': {
    'object': {
      'appellation': {
        // ...
        'string': 'Foo',
      }
    }
  },
  '__deleted': 'false',
}
```

#### Out

```json5
// ProjectRdfKey
{
  'project_id': 567,
  'turtle': '<http://geovistory.org/resource/i1761647> <https://ontome.net/ontology/p1113> "Foo"^^<http://www.w3.org/2001/XMLSchema#string> .'
}
```

```json5
// ProjectRdfValue
{
  'operation': 'insert'
}
```

### LangString

#### In

```json5
// ProjectStatementValue:
{
  'project_id': 567,
  'statement_id': 345,
}
```

```json5
// ProjectStatementValue:
{
  'project_id': 567,
  'subject_id': 'i1761647',
  'property_id': 1113,
  'object_id': 'i2255949',
  'statement': {
    'object': {
      'lang_string': {
        // ...
        'string': 'Bar',
        'fk_language': 19703,
      }
    }
  },
  '__deleted': 'false',
}
```

`fk_language` refers to an internal identifier of the Geovistory Toolbox. These identifiers have been historically
grown.

`fk_language` can be converted to two character language codes using a HashMap derived from `languageMap` in
`org.geovistory.toolbox.streams.lib.Utils`.

In case the HashMap does not return a language code, use `fk_language`.
This case is expected to be extremely rare. Future improvement would involve a join of lang_string with language
in `org.geovistory.toolbox.streams.statement.enriched.processors.StatementEnriched` in order to retrieve a language
code.

#### Out

```json5
// ProjectRdfKey
{
  'project_id': 567,
  'turtle': '<http://geovistory.org/resource/i1761647> <https://ontome.net/ontology/p1113> "Bar"@it .'
}
```

```json5
// ProjectRdfValue
{
  'operation': 'insert'
}
```

### Place

#### In

```json5
// ProjectStatementValue:
{
  'project_id': 567,
  'statement_id': 345,
}
```

```json5
// ProjectStatementValue:
{
  'project_id': 567,
  'subject_id': 'i1761647',
  'property_id': 1113,
  'object_id': 'i2255949',
  'statement': {
    'object': {
      'place': {
        // ...
        'geo_point': {
          "io.debezium.data.geometry.Geography": {
            "wkb": "AQEAACDmEAAA9DRgkPTJAkB9zAcEOm1IQA==",
            "srid": {
              "int": 4326
            }
          }
        },
      }
    }
  },
  '__deleted': 'false',
}
```

Remarks on `geo_point`

- The `geo_point` is serialized as `io.debezium.data.geometry.Geography`.
- `geo_point.wkb` is a base64 encoded Extended-well-known-binary produced by PostGIS. This needs to be converted to
  well-known-text (WKT); It might be sufficient to decode with `java.util.Base64.getDecoder().decode();`. See examples
  in `org.geovistory.toolbox.streams.lib.GeoUtilsTest`. The resulting WKT should be identical as the output of PostGIS
  ST_AsText().
- `geo_point.srid` is always 4326

For writing tests, this might be helpful:

- `org.geovistory.toolbox.streams.statement.enriched.processors.StatementEnrichedTest`

#### Out

```json5
// ProjectRdfKey
{
  'project_id': 567,
  'turtle': '<http://geovistory.org/resource/i1761647> <https://ontome.net/ontology/p1113> "<http://www.opengis.net/def/crs/EPSG/0/4326>POINT(2.348611 48.853333)"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .'
}
```

```json5
// ProjectRdfValue
{
  'operation': 'insert'
}
```


### TimePrimitive

#### In

```json5
// ProjectStatementValue:
{
  'project_id': 567,
  'statement_id': 345,
}
```

```json5
// ProjectStatementValue:
{
  'project_id': 567,
  'subject_id': 'i1761647',
  'property_id': 1113,
  'object_id': 'i2255949',
  'statement': {
    'object': {
      // ...
      'time_primitive': {
        "pk_entity": 769073,
        "duration": {
          "string": "1 day"
        },
        "fk_class": {
          "int": 335
        },
        "julian_day": {
          "int": 2304386
        },
        "calendar": "gregorian"
      }
    }
  },
  '__deleted': 'false',
}

```

#### Out

TODO

### Dimension

TODO

### Cell

TODO

### Digital

TODO

