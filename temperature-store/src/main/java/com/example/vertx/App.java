package com.example.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneId;

public class App extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(App.class);
    private static final int httpPort = Integer.parseInt(System.getenv().getOrDefault("HTTP_PORT", "7000"));

    private PgPool pgPool;

    @Override
    public void start(Promise<Void> startPromise) {
        pgPool = PgPool.pool(vertx, new PgConnectOptions()
                .setHost("127.0.0.0")
                .setUser("quarkus_user")
                .setPassword("quarkus_pass")
                .setDatabase("quarkus_test"), new PoolOptions());

        vertx.eventBus().consumer("temperature.updates", this::recordTemperatures);

        Router router = Router.router(vertx);
        router.get("/all").handler(this::getAllData);
        router.get("/for/:uuid").handler(this::getData);
        router.get("/last-5-minutes").handler(this::getLastFiveMinutes);

        vertx.createHttpServer()
                .requestHandler(router)
                .listen(httpPort)
                .onSuccess(ok -> {
                    logger.info("http server running: http://127.0.0.1:" + httpPort);
                    startPromise.complete();
                })
                .onFailure(startPromise::fail);
    }

    private void getData(RoutingContext routingContext) {
        logger.info("Processing get data request from " + routingContext.request().remoteAddress());

        pgPool.preparedQuery("select tstamp, value from temperature_records where uuid = $1")
                .execute(Tuple.of(routingContext.request().getParam("uuid")))
                .onSuccess(rows -> {
                    JsonArray jsonArray = new JsonArray();
                    for (Row row : rows) {
                        jsonArray.add(new JsonObject()
                                .put("temperature", row.getDouble("value"))
                                .put("timestamp", row.getOffsetTime("tstamp").toString()));
                    }
                    routingContext.response()
                            .putHeader("Content-Type", "application/json")
                            .setStatusCode(200)
                            .end(new JsonObject().put("uuid", routingContext.request().getParam("uuid")).put("data", jsonArray).encode());
                })
                .onFailure(failure -> {
                    logger.error("get all data failed", failure);
                    routingContext.fail(500);
                });
    }

    private void getAllData(RoutingContext routingContext) {
        logger.info("Processing get all data request from " + routingContext.request().remoteAddress());

        pgPool.preparedQuery("select * from temperature_records")
                .execute()
                .onSuccess(rows -> {
                    JsonArray jsonArray = new JsonArray();
                    for (Row row : rows) {
                        jsonArray.add(new JsonObject()
                                .put("uuid", row.getString("uuid"))
                                .put("temperature", row.getDouble("value"))
                                .put("timestamp", row.getOffsetTime("tstamp").toString()));
                    }
                    routingContext.response()
                            .putHeader("Content-Type", "application/json")
                            .setStatusCode(200)
                            .end(new JsonObject().put("data", jsonArray).encode());
                })
                .onFailure(failure -> {
                    logger.error("get all data failed", failure);
                    routingContext.fail(500);
                });
    }

    private void getLastFiveMinutes(RoutingContext routingContext) {
        logger.info("Processing get last 5 min data request from " + routingContext.request().remoteAddress());

        pgPool.preparedQuery("select * from temperature_records where tstamp >= now() - INTERVAL '5 minutes'")
                .execute()
                .onSuccess(rows -> {
                    JsonArray jsonArray = new JsonArray();
                    for (Row row : rows) {
                        jsonArray.add(new JsonObject()
                                .put("uuid", row.getString("uuid"))
                                .put("temperature", row.getDouble("value"))
                                .put("timestamp", row.getOffsetTime("tstamp").toString()));
                    }
                    routingContext.response()
                            .putHeader("Content-Type", "application/json")
                            .setStatusCode(200)
                            .end(new JsonObject().put("data", jsonArray).encode());
                })
                .onFailure(failure -> {
                    logger.error("get all data failed", failure);
                    routingContext.fail(500);
                });
    }

    private void recordTemperatures(Message<JsonObject> message) {
        Timestamp timestamp = new Timestamp(message.body().getLong("timestamp"));
        OffsetDateTime offsetDateTime = OffsetDateTime.ofInstant(timestamp.toInstant(), ZoneId.systemDefault());

        pgPool.preparedQuery("insert into temperature_records (uuid, tstamp, value) values ($1,$2,$3)")
                .execute(Tuple.of(message.body().getString("uuid"), offsetDateTime, message.body().getDouble("temperature")))
                .onSuccess(rows -> logger.info("successfully saved in database " + message.body()))
                .onFailure(failure -> logger.error("failed to save the data in database " + failure));

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
