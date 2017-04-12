# mongo-adapter

## How To Deploy

### Upload jars to bucket

```
mvn clean package -DskipTests
curl -v -X PUT -T target/original-mongo-adapter-1.0-SNAPSHOT.jar http://w:write@localhost:1234/mongo/original-mongo-adapter-1.0-SNAPSHOT.jar
curl -v -X PUT -T target/mongo-adapter-1.0-SNAPSHOT.jar http://w:write@localhost:1234/mongo/mongo-adapter-1.0-SNAPSHOT.jar
```

### Create Scripts in EXASOL

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

### Create Virtual Schema

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

