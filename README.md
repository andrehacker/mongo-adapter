# MongoDB Adapter For EXASOL Virtual Schemas

This adapter let's you access MongoDB collections from a EXASOL virtual schema.

The adapter has following features
* Two different modes for accessing the data
  - JSON mode, where each collection is mapped to a table with a single column containing the JSON representation of the documents
  - MAPPED mode, where you can specify which fields of a document shall be mapped to columns in virtual tables.

To Be Considered
* EXASOL cannot distinguish between empty strings '' and NULL. However, in MongoDB a document could either not contain a field, or contain it with empty string. The adapter does not distinguish these two cases. I.e. IS NULL filter means the field does not exist or it exists with empty string. IS NOT NULL means the field exists and has a value different from empty string.
* Limit with list wildcards: The pushdown to MongoDB will request the specified number of documents, however, only if there are arrays exploded to multiple rows, not all documents might be required (some loaded without purpose)

## How To Deploy

### 1. Prerequisites:
* EXASOL >= 6.0 Advanced Edition or Free Small Business Edition
* All EXASOL nodes must be able to connect to MongoDB
* Java 8 and Maven

### 2. Clone And Build

First you have to clone the repository and build it:
```
git clone https://github.com/andrehacker/mongo-adapter.git
cd mongo-adapter/
mvn clean -DskipTests package
```

### 2. Upload Jars To Bucket
Now upload the resulting jars to a bucket of your choice. You have to adapt the port of the BucketFS (```1234```), the bucket name (```mongo```) and the write password (```write```).
```
curl -v -X PUT -T target/original-mongo-adapter-1.0-SNAPSHOT.jar http://w:write@localhost:1234/mongo/original-mongo-adapter-1.0-SNAPSHOT.jar
curl -v -X PUT -T target/mongo-adapter-1.0-SNAPSHOT.jar http://w:write@localhost:1234/mongo/mongo-adapter-1.0-SNAPSHOT.jar
```

### Create Scripts in EXASOL
Now run the following commands in EXASOL. You have to adapt the name of the bucketfs (```bfsdefault```) and bucket (```mongo```).
```sql
CREATE SCHEMA IF NOT EXISTS MONGO_ADAPTER;

CREATE OR REPLACE JAVA ADAPTER SCRIPT MONGO_ADAPTER AS
%scriptclass com.exasol.mongo.adapter.MongoAdapter;
%jar /buckets/bfsdefault/mongo/mongo-adapter-1.0-SNAPSHOT.jar;
%jar /buckets/bfsdefault/mongo/original-mongo-adapter-1.0-SNAPSHOT.jar;
/

CREATE OR REPLACE JAVA SET SCRIPT READ_COLLECTION_MAPPED (request varchar(2000000)) EMITS (...) AS
%scriptclass com.exasol.mongo.scriptclasses.ReadCollectionMapped;
%jar /buckets/bfsdefault/mongo/mongo-adapter-1.0-SNAPSHOT.jar;
%jar /buckets/bfsdefault/mongo/original-mongo-adapter-1.0-SNAPSHOT.jar;
/
```

## Example

Now we are ready to create a virtual schema. The following example demonstrates the functionality.

Assume we have two collections in MongoDB, with a typical document looking like follows:


Json, for exploration:
```sql
CREATE VIRTUAL SCHEMA VS_MONGO_JSON using MONGO_ADAPTER.MONGO_ADAPTER with
 MONGO_HOST = 'submit4.gelb.exasol.com'
 MONGO_PORT = '27017'
 MONGO_DB = 'tests_database'
 --IGNORE_COLLECTION_CASE = 'true'
 MODE = 'JSON'
 MAX_RESULT_ROWS = '1000';
```

Mapped:
```sql
CREATE VIRTUAL SCHEMA VS_MONGO_MAPPED using MONGO_ADAPTER.MONGO_ADAPTER with
 MONGO_HOST = 'submit4.gelb.exasol.com'
 MONGO_PORT = '27017'
 MONGO_DB = 'tests_database'
 IGNORE_COLLECTION_CASE = 'true'
 MAX_RESULT_ROWS = '1000'
 MODE = 'MAPPED'
 MAPPING = '{
"tables": [
  {
    "collectionName": "comments",
    "tableName": "COMMENTS",
    "columns": [
        {
          "jsonpath": "_id",
          "columnName": "OBJECT_ID",
          "type": "objectid"
        },
        {
          "jsonpath": "time",
          "columnName": "TIME_CREATED",
          "type": "double"
        },
        {
          "jsonpath": "author",
          "columnName": "AUTHOR",
          "type": "string"
        },
        {
          "jsonpath": "jobid",
          "columnName": "JOBID",
          "type": "string"
        },
        {
          "jsonpath": "action",
          "columnName": "USER_ACTION",
          "type": "string"
        }
    ]
  },
  {
    "collectionName": "testsets",
    "tableName": "TESTSETS",
    "columns": [
        {
          "jsonpath": "shortid",
          "columnName": "TESTSET_ID",
          "type": "string"
        },
        {
          "jsonpath": "uuid",
          "columnName": "TESTSET_UUID",
          "type": "string"
        },
        {
          "jsonpath": "nightly_owner",
          "columnName": "NIGHTLY_OWNER",
          "type": "string"
        },
        {
          "jsonpath": "nightly_wip",
          "columnName": "NIGHTLY_WIP",
          "type": "boolean"
        },
        {
          "jsonpath": "testobject_name",
          "columnName": "TESTOBJECT_NAME",
          "type": "string"
        },
        {
          "jsonpath": "buildconfig.Alias",
          "columnName": "BUILDCONFIG_ALIAS",
          "type": "string"
        },
        {
          "jsonpath": "time_created",
          "columnName": "TIME_CREATED",
          "type": "double"
        },
        {
          "jsonpath": "time_updated",
          "columnName": "TIME_UPDATED",
          "type": "double"
        },
        {
          "jsonpath": "user",
          "columnName": "SUBMIT_USER",
          "type": "string"
        },
        {
          "jsonpath": "buildconfig",
          "columnName": "BUILDCONFIG",
          "type": "document"
        },
        {
          "jsonpath": "jobs",
          "columnName": "JOBS",
          "type": "array"
        },
        {
          "jsonpath": "testreport.testreport.testsuite",
          "columnName": "TESTRESULTS",
          "type": "array"
        }
    ]
  },
  {
    "collectionName": "testjobs",
    "tableName": "TESTJOBS",
    "columns": [
        {
          "jsonpath": "@jobid",
          "columnName": "JOB_ID",
          "type": "string"
        },
        {
          "jsonpath": "testset",
          "columnName": "TESTSET_ID",
          "type": "string"
        },
        {
          "jsonpath": "@name",
          "columnName": "NAME",
          "type": "string"
        },
        {
          "jsonpath": "@status",
          "columnName": "STATUS",
          "type": "string"
        },
        {
          "jsonpath": "time_created",
          "columnName": "TIME_CREATED",
          "type": "double"
        },
        {
          "jsonpath": "time_updated",
          "columnName": "TIME_UPDATED",
          "type": "double"
        },
        {
          "jsonpath": "time_started",
          "columnName": "TIME_STARTED",
          "type": "double"
        },
        {
          "jsonpath": "time_stopped",
          "columnName": "TIME_STOPPED",
          "type": "double"
        },
        {
          "jsonpath": "user",
          "columnName": "SUBMIT_USER",
          "type": "string"
        }
    ]
  }
]
}';
```
