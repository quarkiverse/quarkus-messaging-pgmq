package io.quarkiverse.reactive.messaging.pgmq.connector.client;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import io.vertx.mutiny.sqlclient.Pool;

@ApplicationScoped
public class PgPoolProvider {

    static Pool pool;

    @Produces
    Pool pool() {
        return pool;
    }
}
