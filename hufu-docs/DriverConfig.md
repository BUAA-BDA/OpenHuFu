# Configuration of Driver

## Driver Configuration Example

Here we use the following example to explain configuration items of the driver.
Note that the example is a configuration on postgis drivers, other driver's configuration is similar.

```json
{
    "port": 54678,  # the socket port number that the driver bind to
    "url": "jdbc:postgresql://localhost:5432/osm_db",  # the url of postgresql jdbc
    "catalog": "osm_db",  # database name
    "user": "hufu", # user of postgresql
    "passwd": "hufu", # password of the user
    "tables": [  # Regitered table list, only the tables appear here will be recognized by Hu-Fu, other tables are invisible to the outside.
        {
            "name": "osm_a_1", # table name
            "columns": [ # column security level definition, columns not listed here are set to public by default.
                {
                    "name": "id", # column name
                    "level": "protected" # column security level, details of security level will be discuss later.
                },
                {
                    "name": "location",
                    "level": "private"
                }
            ]
        },
        {
            "name": "osm_b_1",
            "columns": []
        }
    ]
}
```

## Security Level

Hu-Fu has 4 security level on columns.

- public: the column is public to others, and can be scanned directly(default)
- protected: the column can be scanned but data ownership will be hidden from user and other silos
- private: the column can't be scan directly, and only statistics like COUNT, SUM is allow.
- hide: the column invisible to others, and prohibit any operations on the column.
