AnalysisStatement

- pk_entity int
- fk_project int, null
- project int
- fk_property int
- fk_object_info int
- fk_subject_info int
- ord_num_of_domain int
- ord_num_of_range int
- is_in_project_count int
- object_info_value ObjectInfoValue

ObjectInfoValue
```json5
{
  'string': {},
  'geometry': {},
  'language': {},
  'timePrimitive': {},
  'langString': {},
  'dimension': {},
  'cell': {}
}
```

String
```json5
{
  "string": {
    "string": "può essere un anno che io l’ho veduto et lo vidi qua in Venetia,",
    "fkClass": 456,
    "pkEntity": 866512
  }
}
```
Geometry
```json5
{
  "geometry": {
    "fkClass": 51,
    "geoJSON": {
      "type": "Point",
      "coordinates": [
        12.335,
        45.441111
      ]
    },
    "pkEntity": 755491
  }
}
```


Language
```json
{
  "language": {
    "label": "Italian",
    "fkClass": 54,
    "iso6391": "it ",
    "iso6392b": "ita",
    "iso6392t": "ita",
    "pkEntity": 19703
  }
}
```

TimePrimitive
```json5

{
  "timePrimitive": {
    "to": {
      "calJulian": "1560-01-01",
      "julianDay": 2290848,
      "calGregorian": "1560-01-11",
      "julianSecond": 197929267200,
      "calGregorianIso8601": "1560-01-11T00:00:00Z"
    },
    "from": {
      "calJulian": "1559-01-01",
      "julianDay": 2290483,
      "calGregorian": "1559-01-11",
      "julianSecond": 197897731200,
      "calGregorianIso8601": "1559-01-11T00:00:00Z"
    },
    "label": "1559-01-01 (1 year)",
    "fkClass": 335,
    "calendar": "julian",
    "duration": "1 year",
    "pkEntity": 81429,
    "julianDay": 2290483
  }
}
```

LangString
```json5
{
  "langString": {
    "string": "Created by J.C. 19/02/2020 ",
    "fkClass": 785,
    "pkEntity": 860944,
    "fkLanguage": 19703
  }
}
```


Dimension
```json5
{
  "dimension": {
    "fkClass": 52,
    "pkEntity": 3169537,
    "numericValue": 35,
    "fkMeasurementUnit": 3166412
  }
}
```
Cell
```json5
{
  "cell": {
    "fkRow": 24810299,
    "pkCell": 29609231,
    "fkClass": 521,
    "fkColumn": 25315168,
    "stringValue": "Married"
  }
}
```

```json5
{
  "cell": {
    "fkRow": 24832201,
    "pkCell": 29781095,
    "fkClass": 521,
    "fkColumn": 25315299,
    "numericValue": 1390
  }
}
```

