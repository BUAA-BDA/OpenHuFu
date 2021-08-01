# Configuration of CLI

## CLI Configuration Example

Here we use a example to explain configuration items of CLI.
Note that all drivers provides a unified inerface fot the CLI, so the configuration of CLI is completely decoupled from the databases used by silos.

```json
{
  "version": "1.0", # don't change
  "defaultSchema": "points", # don't change
  "schemas": [
    {
      "name": "points", # the schema name
      "type": "custom", # don't change
      "factory": "group.bda.federate.sql.schema.FederateSchemaFactory", # don't change
      "operand": {
        "endpoints": ["localhost:54678", "localhost:54679", "localhost:54680", "localhost:54681"] # The ip of each silo and the port bound by dirver(endpoint of silo's driver)
      },
      "tables": [
        {
          "name": "osm_a", # The federated view name
          "factory": "group.bda.federate.sql.table.FederateTableFactory", # don't change
          "operand": {
            "feds": [ # each item in this list represents a silo's local table
              {
                "endpoint": "localhost:54678", # the endpoint of silo's driver
                "name": "osm_a_1" # the local table name
              },
              {
                "endpoint": "localhost:54679",
                "name": "osm_a_2"
              },
              {
                "endpoint": "localhost:54680",
                "name": "osm_a_3"
              },
              {
                "endpoint": "localhost:54681",
                "name": "osm_a_4"
              }
            ]
          }
        }
      ]
    }
  ]
}

```