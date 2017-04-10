package com.exasol;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import org.bson.Document;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;


public class App 
{

    static Block<Document> printBlock = new Block<Document>() {
        //@Override
        public void apply(final Document document) {
            System.out.println(document.toJson());
        }
    };

    static Block<Document> printTestset = new Block<Document>() {
        //@Override
        public void apply(final Document document) {
            //System.out.println("testobject_name: " + document.getString("testobject_name"));
            System.out.println("buildconfig.Alias: " + document.get("buildconfig", Document.class).getString("Alias"));
            System.out.println("jobs: " + document.get("jobs").toString());
            document.get("b");
        }
    };

    public static void main( String[] args )
    {
        MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
        MongoDatabase database = mongoClient.getDatabase("test");  // tests_database

        long start = LocalDateTime.of(2017,3,22,18,0).toEpochSecond(ZoneOffset.ofHours(0)); // 2017,3,22,18,0
        long end = LocalDateTime.of(2017,3,23,18,0).toEpochSecond(ZoneOffset.ofHours(0));  // 2017,3,23,18,0

        MongoCollection<Document> testsets = database.getCollection("testsets");
        System.out.println("Testsets Count: " + testsets.count());

        // Nightly testsets
        testsets.find(
                and(
                        gte("time_created", start),
                        gte("time_created", end),
                        eq("nightly_wip", false)
                )
                )
                .projection(fields(include("buildconfig.Alias", "testobject_name", "jobs"), excludeId()))
                .sort(Sorts.ascending("testobject_name"))
                .forEach(printTestset);
        // Failed jobs
    }
}
