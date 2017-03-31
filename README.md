# mongo-adapter

## How To Deploy

```sql
CREATE OR REPLACE JAVA SET SCRIPT READ_COLLECTION_MAPPED (host varchar(200), port integer, db varchar(200), collection varchar(200)) EMITS (...) AS
%scriptclass com.exasol.mongo.scriptclasses.ReadCollectionMappedOld;
%jar /buckets/bucketfs1/mongo/mongo-adapter-1.0-SNAPSHOT.jar;
/
```
