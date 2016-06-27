package com.heroku.diffcalculator.definition.mongo;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MongoCollections {

  @Autowired
  public MongoCollections(MongoConfig config) {
    System.out.println("aaaaaaaaaaaa");
    createDiffCollections(config);
    System.out.println("bbbbbbbbbbbb");
    createMasterCollections(config);
    System.out.println("cccccccccccc");
  }

  private void createDiffCollections(MongoConfig config) {
    MongoDatabase diffDatabase
            = config.diffClient
            .getDatabase(config.diffDatabaseName);

    Set<String> existCollectionNames
            = StreamSupport.stream(
                    diffDatabase.listCollectionNames().spliterator(), false)
            .collect(Collectors.toSet());

    Document settings = config.getMasterSettings();
    List<Map<String, Object>> definedCollections
            = settings.get("collections", List.class);

    definedCollections.stream()
            .map((collection) -> (String) collection.get("collectionName"))
            .filter((collectionName)
                    -> (!existCollectionNames.contains("diff_" + collectionName)))
            .forEach((collectionName) -> {
              diffDatabase.createCollection("diff_" + collectionName,
                      new CreateCollectionOptions()
                      .sizeInBytes(16777216).capped(true).maxDocuments(200));
            });
  }

  private void createMasterCollections(MongoConfig config) {
    MongoDatabase masterDatabase
            = config.masterClient
            .getDatabase(config.masterDatabaseName);

    Set<String> existCollectionNames
            = StreamSupport.stream(
                    masterDatabase.listCollectionNames().spliterator(), false)
            .collect(Collectors.toSet());

    Document settings = config.getMasterSettings();
    List<Map<String, Object>> definedCollections
            = settings.get("collections", List.class);

    definedCollections.stream()
            .map((collection) -> (String) collection.get("collectionName"))
            .filter((collectionName)
                    -> (!existCollectionNames.contains("master_" + collectionName)))
            .forEach((collectionName) -> {
              masterDatabase.createCollection("master_" + collectionName,
                      new CreateCollectionOptions()
                      .sizeInBytes(8388608).capped(true).maxDocuments(10));
            });
  }
}
