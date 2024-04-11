# App Items

This app creates two categories topics: topics with entities and topics with edges. Each category consists of four
topics, providing data for the four publication targets.

## Output topics

### Entities

Entity topics bear two informations:

- The existence of a project-entity in a publication target (you can join information about an entity based on these
  topics: in case the record is marked as deleted, you can consider all information about this entity as deleted)
- The class id of an entity

The entities are pushed to the following topics, according to their visibility settings.

Topic names:

- {app_id}_toolbox_project_entities
- {app_id}_toolbox_community_entities
- {app_id}_public_project_entities
- {app_id}_public_community_entities

Data model (examples):

```json5
// ProjectEntityKey
{
  "entity_id": "i880985",
  "project_id": 591
}
```

```json5
// ProjectEntityValue
{
  "__deleted": {
    "boolean": true
  },
  "class_id": 899,
  "entity_id": "i880985",
  "project_id": 591
}
```

Remark:

- **project_id**: In the two toolbox topics, project_id is always 0.

- **deleted**: Entities are marked as deleted for several reasons:
  - The entity is removed from project
    (in case of project topics)
  - The entity is removed from all project (in case of community topics)
  - The entities was marked as hidden for a publication target
  - The entity was removed from data base

### Edges

Edges are deduced from statements: They materialize the implicit inverse property of statements
with an object entity. In other words: Each statement that has an entity (not a literal) object
leads to two edges. Instead of subject-property-object (statement), an edge consist of
source–property+direction–target. This allows the creation of entity labels and the publication of inverse  
properties in RDF for down-stream apps.

Topic names:

- {app_id}_toolbox_project_edges
- {app_id}_toolbox_community_edges
- {app_id}_public_project_edges
- {app_id}_public_community_edges

Data model (examples):

```json5
// Key (String)
"toolbox_i880929_1111_false_i880935"
```

```json5
// EdgeValue
{
  // if true, not in the publication target
  "deleted": false,
  // if true, source is subject of original statement, otherwise object
  "is_outgoing": true,
  "modified_at": {
    "string": "2024-04-10T14:05:59.142010Z",
  },
  // The order of the edge within all edges with the same source_id, property_id and is_outgoing
  // The order is relevant for entity label and full text creation.
  // In case of project topics, this is defined by the users
  // In case of community topics, this is the average ord_num the edge has across its projects
  // number of projects having this edge.
  "ord_num": null,
  // In case of project topics this is 1
  "project_count": 0,
  // The id of the project
  // In case of community topics this is 0
  "project_id": 591,
  // Property id originating from OntoME
  "property_id": 1864,
  // The source entity
  "source_entity": {
    // visibility for public_community
    "community_visibility_data_api": true,
    // visibility for toolbox_community
    "community_visibility_toolbox": true,
    // irrelevant
    "community_visibility_website": true,
    // Class id originating from OntoME
    "fk_class": 899,
    // ID of source entity
    "pk_entity": 881015,
  },
  // ID of source entity prefixed with "i"
  "source_id": "i881015",
  // The project entity with project visibility settings
  "source_project_entity": {
    "org.geovistory.toolbox.streams.avro.EntityValue": {
      // Class id originating from OntoME
      "class_id": 899,
      // visibility for public_community
      "community_visibility_data_api": true,
      // visibility for toolbox_community
      "community_visibility_toolbox": true,
      // irrelevant
      "community_visibility_website": true,
      // always false
      "deleted": false,
      // ID prefixed with "i"
      "entity_id": "i881015",
      "project_id": 591,
      // visibility for public_project
      "project_visibility_data_api": false,
      // irrelevant
      "project_visibility_website": false,
    },
  },
  // id of the edge
  "statement_id": 881035,
  // ID of target entity prefixed with "i"
  "target_id": "i881034",
  // target node, can contain literal or entity
  "target_node": {
    // literal, here a string
    "appellation": {
      "org.geovistory.toolbox.streams.avro.Appellation": {
        // Class id originating from OntoME
        "fk_class": 339,
        "pk_entity": 881034,
        // the literal value
        "string": "T3 7",
      },
    },
    "cell": null,
    "class_id": 339,
    "digital": null,
    "dimension": null,
    "entity": null,
    "id": "i881034",
    // label deduced from literal
    "label": {
      "string": "T3 7",
    },
    "lang_string": null,
    "language": null,
    "place": null,
    "time_primitive": null,
  },
  // target project entity 
  // is not null in project topics for edges with an entity in the project
  "target_project_entity": null
}
```

# Test

## Start (in dev mode)

Dev mode runs against existing kafka brokers and schema registry.

Download dev-stack

```text
git clone https://github.com/geovistory/dev-stack.git

cd dev-stack

bash ./scripts/build

# wait until stack up and running
```

Start dev mode

```bash
# from this directory
quarkus dev
```

Open redpanda console from dev-stack and see topics:

http://localhost:1120/topics

At the same time, continuous testing is enabled. Hit 'd' in the terminal and navigate to Continuous Testing.
If you change (test-)code, the test will re-run.

## Test (continuous)

Tests launch redpanda using test containers. Only docker needed, no additional setup needed.

Start Continuous Testing

### With cli

Make sure to comment this line in application.properties:
`#quarkus.test.flat-class-path=true`

```bash
# from this directory
quarkus test
```

### With IDE

In IntelliJ navigate to the Test class and use the UI to start tests.
Make sure to uncomment this line in application.properties:
`quarkus.test.flat-class-path=true`
