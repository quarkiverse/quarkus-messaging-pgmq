package io.quarkiverse.messaging.pgmq.connector;

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
