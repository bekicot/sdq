# Stackexchange Dump Query

Stackexchange dump query. Export XML dump from stackexchange to SQLite.
It will create corresponding table for each files.

## Planned Feature
- query stackexchange xml dump using command line.

## Usage

Export to sqlite db
```
sdq export Users.xml
```

By default it will be exported to `stackexchange.db.sqlite`. It will REPLACE the table with the new data if it already exists.

## Tested DB
- startups.stackexchange.com (Deleted Beta)
