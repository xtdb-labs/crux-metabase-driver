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

- [ ] Date types (if a field is set as Date or DateTime, all queries using it will fail)

# What else ?

- [ ] Reduce driver size (100 Mo today), by removing hive and sparksql dependencies