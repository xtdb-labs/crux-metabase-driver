# Before anything else !

At every time you upload a new version of the driver, please think about :
- Restarting metabase service,
- Dropping the datasource in Metabase, and recreating it (with the new version of the driver applied)

If you don't do that, you'll become crazy very quickly...

# What is working ?

- [x] Connect to Dremio
- [x] Get All schema list (physical and virtual)
- [x] Get All tables
- [x] Get All fields
- [x] Get automatic x-ray
- [x] Query SQL manually

# What should work differently ?

- [ ] Get only provided schema, and not all
- [ ] Get correct field types (and not only type/*)

# What doesn't work at all ?

- [ ] Ask SQL questions doesn't work, because of non aliasing schema + table in front of column names

# What else ?

- [x] Reduce driver size (100 Mo today), by removing hive and sparksql dependencies > Now 26Mo with p6spy for logging sql requests
- [x] Get some JDBC logs (enable debug mode when connecting to database, it creates a spy.log file > [p6spy](https://github.com/p6spy/p6spy))