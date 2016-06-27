package com.heroku.diffcalculator.sources.polling;

import com.heroku.diffcalculator.definition.mongo.MongoConfig;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.rx.ReactiveCamel;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import rx.Observable;

@Component
public class FemaleSeiyuCategoryMembers extends RouteBuilder {

  final String collectionName = "female_seiyu_category_members";
  @Autowired
  MongoConfig config;

  @Autowired
  public FemaleSeiyuCategoryMembers(
          CamelContext context, MongoConfig config) {
    ReactiveCamel rx = new ReactiveCamel(context);
    Observable<Document> obs = rx.toObservable("direct:snapshot_" + collectionName, Document.class).buffer(2, 1)
            .map((List<Document> list) -> {
              Document master = list.get(1);
              List<Map<String, String>> prev = list.get(0).get("data", List.class);
              List<Map<String, String>> next = master.get("data", List.class);
              prev.forEach((map) -> map.put("type", "remove"));
              next.forEach((map) -> map.put("type", "add"));
              List<Map<String, String>> collect = prev.stream().filter((map1) -> {
                final String map1Title = map1.get("title");
                return !next.stream().anyMatch((map2) -> map2.get("title").equals(map1Title));
              }).collect(Collectors.toList());
              next.stream().filter((map2) -> {
                final String map2Title = map2.get("title");
                return !prev.stream().anyMatch((map1) -> map1.get("title").equals(map2Title));
              }).forEach(collect::add);
              Document document = new Document();
              if (!collect.isEmpty()) {
                document.put("data", collect);
                document.put("creationDate", new Date());
                document.put("master", master);
              }
              return document;
            });
    rx.sendTo(obs, "direct:result_" + collectionName);
  }

  @Override
  public void configure() throws Exception {
    fromF("mongodb:snapshot?database=%s"
            + "&collection=%s"
            + "&persistentId=%sTracker"
            + "&createCollection=false&"
            + "tailTrackIncreasingField=creationDate"
            + "&persistentTailTracking=true",
            config.snapshotDatabaseName,
            "snapshot_" + collectionName,
            "snapshot_" + collectionName)
            .routeId("mongo_" + collectionName)
            .toF("direct:snapshot_%s", collectionName);

    from("direct:result_" + collectionName)
            .routeId("result_" + collectionName)
            .to("log:foo")
            .filter((Exchange exchange)
                    -> exchange.getIn().getBody(Document.class).containsKey("data"))
            .wireTap("direct:diff_" + collectionName)
            .wireTap("direct:master_" + collectionName);

    from("direct:diff_" + collectionName)
            .routeId("save_diff_" + collectionName)
            .process((Exchange exchange) -> {
              Document diff = exchange.getIn().getBody(Document.class);
              diff.remove("master");
              exchange.getIn().setBody(diff);
              System.out.println("diff insert document..." + diff);
            })
            .toF("mongodb:diff?database=%s&collection=%s&operation=insert",
                    config.diffDatabaseName,
                    "diff_" + collectionName);

    from("direct:master_" + collectionName)
            .routeId("save_master_" + collectionName)
            .process((Exchange exchange) -> {
              Document diff = exchange.getIn().getBody(Document.class);
              Document master = diff.get("master", Document.class);
              exchange.getIn().setBody(master);
              System.out.println("master insert document..." + master);
            })
            .toF("mongodb:master?database=%s&collection=%s&operation=insert",
                    config.masterDatabaseName,
                    "master_" + collectionName);
  }
}
