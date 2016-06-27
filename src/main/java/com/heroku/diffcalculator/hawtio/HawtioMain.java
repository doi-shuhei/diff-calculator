package com.heroku.diffcalculator.hawtio;

import com.heroku.diffcalculator.definition.mongo.MongoConfig;
import io.hawt.embedded.Main;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class HawtioMain {

  @Autowired
  public HawtioMain(MongoConfig config) throws Exception {
    Main main = new Main();
    System.setProperty("hawtio.authenticationEnabled", "false");
    String port = System.getenv("PORT");
    if (port == null) {
      port = "3939";
    }
    main.setPort(Integer.parseInt(port));
    main.setContextPath("/" + config.ownMongoDatabaseName);
    main.setWarLocation("./");
    main.run();
  }
}
