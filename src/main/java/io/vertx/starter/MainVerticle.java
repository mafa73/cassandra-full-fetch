package io.vertx.starter;

import java.util.List;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;

import io.vertx.cassandra.CassandraClient;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

public class MainVerticle extends AbstractVerticle {

  @Override
  public void start(Future<Void> startFuture) {

    final CassandraClient cassandraClient = CassandraClient.create(vertx);

    Future setUp = Future.future(promise -> {
      cassandraClient.execute(" CREATE KEYSPACE IF NOT EXISTS test " +
        "WITH replication = {'class': 'SimpleStrategy' , 'replication_factor': 1}", keyspace -> {

        if (keyspace.succeeded()) {
          cassandraClient.execute("CREATE TABLE IF NOT EXISTS test.test_table (id int, PRIMARY KEY (id))", create -> {
            if (create.succeeded()) {
              promise.complete();
            } else {
              promise.fail(create.cause());
            }
          });
        } else {
          promise.fail(keyspace.cause());
        }
      });
    }).setHandler(result -> {
      if (result.succeeded()) {
        Future.future(createRowsPromise -> {
          BatchStatement batchStatement = new BatchStatement();
          for (int i = 0; i < 7000; i++) {
            final int finalI = i;
            batchStatement.add(
              new SimpleStatement("INSERT INTO test.test_table(id) VALUES(" + finalI + ")")
            );
          }

          cassandraClient.execute(batchStatement, ar -> {
            if (ar.succeeded()) {
              System.out.println("Successfully created 7000 rows");
              createRowsPromise.complete();
            } else {
              System.out.println("Failed to created 7000 rows");
              createRowsPromise.fail(ar.cause());
            }
          });
        });
      } else {
        startFuture.fail(result.cause());
      }
    }).setHandler(ar -> {
      if (ar.succeeded()) {
        System.out.println("Starting server");
        vertx.createHttpServer()
          .requestHandler(req -> fetchRows(cassandraClient, server -> {
            if (server.succeeded()) {
              req.response().end(server.result());
            } else {
              req.response().end(server.cause().toString());
            }
          })).listen(8080);
        startFuture.complete();
      } else {
        startFuture.fail(ar.cause());
      }
    });
  }

  private void fetchRows(final CassandraClient cassandraClient, Handler<AsyncResult<String>> resultHandler) {
    cassandraClient.executeWithFullFetch("SELECT * FROM test.test_table", executeWithFullFetch -> {
      if (executeWithFullFetch.succeeded()) {
        List<Row> rows = executeWithFullFetch.result();
        System.out.println("Number of rows: " + rows.size());
        resultHandler.handle(Future.succeededFuture("Number of rows: " + rows.size()));
      } else {
        System.out.println("Unable to execute the query");
        executeWithFullFetch.cause().printStackTrace();
        resultHandler.handle(Future.failedFuture(executeWithFullFetch.cause()));
      }
    });
  }

}
