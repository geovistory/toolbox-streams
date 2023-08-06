# Topology: ProjectEntityRdfsLabel

This topology produces rdfs:label triples for all project entities.

It does so by reading the input topic `project_entity_label`.

The challenge is to handle the update of an entity's label:
An update needs to be converted into an delete and an insert statement.

```mermaid
flowchart TD
    1a-->2a-->2b-->2c-->3a-->3b
    
    subgraph 1
        1a([1a project_entity_label])
    end
    subgraph __2
        2a([2a GroupBy])
        2b([2b Aggregate])
        2c([2c FlatMap])
    end          
    subgraph __3
        3a([3a To])
        3b[3b project_rdf]
    end
    
```

1a) Create a KStream from `project_entity_label`

2a) GroupBy `ProjectEntityKey`

2b) Aggregate the old and new value to a `ProjectRdfList`

`ProjectRdfList` has a field `items` of type array. The items in the array are of type `ProjectRdfRecord`.

`ProjectRdfRecord`:
- key ProjectRdfKey
- value ProjectRdfValue`

If the old value is not null, add to `ProjectRdfList.items` a `KeyValue<ProjectRdfKey,ProjectRdfValue>.pair()` with
operation `delete` and the triple:

```turtle
<http://geovistory.org/resource/{entityId}> <http://www.w3.org/2000/01/rdf-schema#label> "OLD_LABEL"@^^<http://www.w3.org/2001/XMLSchema#string> .
```

For the new value, add to `ProjectRdfList.items` a `KeyValue<ProjectRdfKey,ProjectRdfValue>.pair()` with
the triple:

```turtle
<http://geovistory.org/resource/{entityId}> <http://www.w3.org/2000/01/rdf-schema#label> "NEW_LABEL"@^^<http://www.w3.org/2001/XMLSchema#string> .
```

Depending on the new values delete flag, operation is `insert` or `delete`.

2b) FlatMap the records of `ProjectRdfList.items`.


3a) To: sink it to `project_rdf`

