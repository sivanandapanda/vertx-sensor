package com.example.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import java.util.Random;
import java.util.UUID;

public class SensorVerticle extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(SensorVerticle.class);
    private static final int httpPort = Integer.parseInt(System.getenv().getOrDefault("HTTP_PORT", "8080"));

    private final String uuid = UUID.randomUUID().toString();
    private double temperature = 21.0;
    private final Random random = new Random();

    @Override
    public void start(Promise<Void> startPromise) {
        vertx.setPeriodic(2000, this::updateTemperature);

        Router router = Router.router(vertx);
        router.get("/data").handler(this::getData);

        vertx.createHttpServer()
                .requestHandler(router)
                .listen(httpPort)
                .onSuccess(ok -> {
                    logger.info("http server running: http://127.0.0.1:"+ httpPort);
                    startPromise.complete();
                })
                .onFailure(startPromise::fail);
    }

    private void getData(RoutingContext context) {
        logger.info("Processing http request from "+ context.request().remoteAddress());
        JsonObject payload = creartePayLoad();
        context.response()
                .putHeader("Content-Type", "application/json")
                .setStatusCode(200)
                .end(payload.encode());
    }

    private JsonObject creartePayLoad() {
        return new JsonObject()
                .put("uuid", uuid)
                .put("temperature", temperature)
                .put("timestamp", System.currentTimeMillis());
    }

    private void updateTemperature(Long id) {
       temperature = temperature + (random.nextGaussian() / 20.0d);
       logger.info("Temperature updated: " + temperature);

       vertx.eventBus()
               .publish("temperature.updates", creartePayLoad());
    }
}
