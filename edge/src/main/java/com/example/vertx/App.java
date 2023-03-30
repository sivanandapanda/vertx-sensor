package com.example.vertx;

import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.core.*;
import io.vertx.core.eventbus.Message;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;

import java.util.HashMap;

public class App extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(App.class);
    private static final int httpPort = Integer.parseInt(System.getenv().getOrDefault("HTTP_PORT", "8081"));

    private final HashMap<String, Object> lastData = new HashMap<>();

    private JsonObject cachedLastFiveMinutes;

    private WebClient webClient;
    private CircuitBreaker circuitBreaker;

    @Override
    public void start(Promise<Void> startPromise) {
        webClient = WebClient.create(vertx);

        circuitBreaker = CircuitBreaker.create("circuitBreaker", vertx);
        circuitBreaker.openHandler(v -> logger.info("Circuit Breaker open"));
        circuitBreaker.halfOpenHandler(v -> logger.info("Circuit Breaker half open"));
        circuitBreaker.closeHandler(v -> logger.info("Circuit Breaker close"));

        vertx.eventBus().consumer("temperature.updates", this::storeUpdate);

        Router router = Router.router(vertx);
        router.get("/latest").handler(this::latestData);
        router.get("/five-minutes").handler(this::fiveMinutes);

        vertx.createHttpServer()
                .requestHandler(router)
                .listen(httpPort)
                .onSuccess(ok -> {
                    logger.info("http server running: http://127.0.0.1:" + httpPort);
                    startPromise.complete();
                })
                .onFailure(startPromise::fail);
    }

    private void latestData(RoutingContext routingContext) {

    }

    private void storeUpdate(Message<JsonObject> message) {

    }

    private void fiveMinutes(RoutingContext routingContext) {
        Future<JsonObject> future = circuitBreaker.execute(promise -> {
            webClient.get(7000, "localhost", "/last-5-minutes")
                    .as(BodyCodec.jsonObject())
                    .timeout(5000)
                    .send()
                    .map(HttpResponse::body)
                    .onSuccess(promise::complete)
                    .onFailure(promise::fail);
        });

        future.onSuccess(json -> {
            logger.info("last 5 min data requested from " + routingContext.request().remoteAddress());
            cachedLastFiveMinutes = json;
            routingContext.response()
                    .setStatusCode(200)
                    .putHeader("Content-Type", "application/json")
                .end(json.encode());
        }).onFailure(failure -> {
            logger.info("last 5 min data served from cache which is requested from " + routingContext.request().remoteAddress());
            if(cachedLastFiveMinutes != null) {
                routingContext.response()
                        .setStatusCode(200)
                        .putHeader("Content-Type", "application/json")
                        .end(cachedLastFiveMinutes.encode());
            } else {
                logger.error("request failed as no response from server in 5secs and no cached data", failure);
                routingContext.fail(500);
            }
        });
    }

    public static void main(String[] args) {
        Vertx.clusteredVertx(new VertxOptions())
                .onSuccess(vertx -> {
                    vertx.deployVerticle(new App());
                    logger.info("vertx server started");
                })
                .onFailure(failure -> logger.error("failed to start vertx server " + failure.getMessage()));
    }
}
