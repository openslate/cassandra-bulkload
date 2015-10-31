====================
Cassandra CSV Loader
====================

-----
Usage
-----
::

    groovy loader.groovy -c /path/to/config.json -o /path/to/create/output -d /path/to/datafile.csv

-------------
Configuration
-------------
::

    {
        "keyspace" : "myKeyspace",
        "table" : "homes",
        "primary_key" : "((city, state), street)",
        "clustering" : "street DESC",                                // optional
        "date_format" : "yyyy-MM-dd HH:mm:ss",
        "fields" : [
                { "name" : "city", "type" : "text" },
                { "name" : "state", "type" : "text" },
                { "name" : "street", "type" : "bigint" },
                { "name" : "house_number", "type" : "int" },
                { "name" : "postal_code", "type" : "text" },
                { "name" : "notes", "type" : "text" },
                { "name" : "latitude", "type" : "decimal" },
                { "name" : "longitude", "type" : "decimal" },
                { "name" : "keywords", "type" : "list<text>" },
        ],
        "filters" : {
                "postal_code" : "{value, line -> sprintf('%0.5d', value)}", // optional
                "notes" : "{value, line -> value ? value.replaceAll('\\\\n', '\\n') : null }", // optional
        }
    }

