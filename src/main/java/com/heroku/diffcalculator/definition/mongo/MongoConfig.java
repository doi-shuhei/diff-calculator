package com.heroku.diffcalculator.definition.mongo;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import org.bson.Document;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MongoConfig {

  public String ownMongoDatabaseName, masterDatabaseName, diffDatabaseName, snapshotDatabaseName;
  public Document masterSettings;
  public MongoClient diffClient, snapshotClient, masterClient;

  public MongoConfig() {
    String ownMongoUri = System.getenv("MONGODB_URI");
    if (ownMongoUri == null) {
      ownMongoUri = System.getenv("DIFF_CALCULATOR_MONGOLAB_URI");
    }
    ownMongoDatabaseName = getDatabaseName(ownMongoUri);
    String masterMongoUri
            = getSettings(ownMongoUri).get("MASTER_MONGOLAB_URI", String.class);

    masterSettings = getSettings(masterMongoUri);
    masterClient = new MongoClient(new MongoClientURI(masterMongoUri));
    masterDatabaseName = getDatabaseName(masterMongoUri);

    String diffMongoUri
            = masterSettings.get("DIFF_MONGOLAB_URI", String.class);

    diffDatabaseName = getDatabaseName(diffMongoUri);
    diffClient = new MongoClient(new MongoClientURI(diffMongoUri));
    String snapshotMongoUri
            = masterSettings.get("SNAPSHOT_MONGOLAB_URI", String.class);

    snapshotDatabaseName = getDatabaseName(snapshotMongoUri);
    snapshotClient = new MongoClient(new MongoClientURI(snapshotMongoUri));
  }

  @Bean(name = "master")
  public MongoClient masterMongoClient() {
    return masterClient;
  }

  @Bean(name = "diff")
  public MongoClient diffMongoClient() {
    return diffClient;
  }

  @Bean(name = "snapshot")
  public MongoClient snapshotMongoClient() {
    return snapshotClient;
  }

  private String getDatabaseName(String mongoUri) {
    if (mongoUri == null) {
      return null;
    } else {
      String[] split = mongoUri.split("/");
      return split[split.length - 1];
    }
  }

  public final Document getSettings(String mongoUri) {
    try (MongoClient mongoClient
            = new MongoClient(new MongoClientURI(mongoUri))) {

      FindIterable<Document> find
              = mongoClient.getDatabase(getDatabaseName(mongoUri))
              .getCollection("settings").find();

      if (find.iterator().hasNext()) {
        return find.first();
      }
    }
    return null;
  }

  public Document getMasterSettings() {
    return masterSettings;
  }
}
