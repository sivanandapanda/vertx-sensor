package com.example.vertx;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;

public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        Vertx.clusteredVertx(new VertxOptions())
                .onSuccess(vertx -> {
                    vertx.deployVerticle(new SensorVerticle());
                    logger.info("vertx server started");
                })
                .onFailure(failure -> logger.error("failed to start vertx server " + failure.getMessage()));
        //vertx.deployVerticle("SensorVerticle", new DeploymentOptions().setInstances(1));
    }
}
