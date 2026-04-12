package io.quarkiverse.messaging.pgmq.client;

import java.util.function.Function;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.SqlClient;

/**
 * A client for interacting with the PGMQ database.
 *
 * This class extends the PGMQClientFunctions to provide a specific implementation for SQL client pool.
 */
public class PGMQClient extends PGMQClientFunctions<Pool> {

    public PGMQClient(Pool client) {
        super(client);
    }

    public Uni<Void> withTransaction(Function<PGMQClientFunctions<SqlClient>, Uni<Void>> action) {
        return client.withTransaction(cnx -> action.apply(new PGMQClientFunctions<>(cnx)));
    }

}
