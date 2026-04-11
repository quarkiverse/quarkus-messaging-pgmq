package io.quarkiverse.reactive.messaging.pgmq.connector.client;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.PoolOptions;

public class PGMQTestBase extends WeldTestBase {

    protected static PostgreSQLContainer<?> postgres;

    private static PGMQClient pgmq;

    private static ExecutionHolder executionHolder;

    @BeforeAll
    static void beforeAll() {
        postgres = new PostgreSQLContainer<>(
                DockerImageName.parse("ghcr.io/pgmq/pg17-pgmq:v1.5.1").asCompatibleSubstituteFor("postgres"))
                .withDatabaseName("testdb")
                .withUsername("postgres")
                .withPassword("postgres")
                .withInitScript("init-pgmq.sql");
        postgres.start();
    }

    @BeforeEach
    void setUp() {
        executionHolder = new ExecutionHolder(Vertx.vertx());
        pgmq = getClient();
        PgPoolProvider.pool = pgmq.getClient();
        addBeans(PgPoolProvider.class);
    }

    @AfterEach
    void tearDown() {
        if (pgmq != null) {
            pgmq.getClient().close().await().indefinitely();
            pgmq = null;
        }
        PgPoolProvider.pool = null;
    }

    public synchronized PGMQClient getClient() {
        if (pgmq == null) {
            PgConnectOptions connectOptions = new PgConnectOptions()
                    .setCachePreparedStatements(true)
                    .setPort(postgres.getMappedPort(5432))
                    .setHost(postgres.getHost())
                    .setDatabase(postgres.getDatabaseName())
                    .setUser(postgres.getUsername())
                    .setPassword(postgres.getPassword());

            PoolOptions poolOptions = new PoolOptions().setMaxSize(5);
            Pool pool = Pool.pool(executionHolder.vertx(), connectOptions, poolOptions);
            pgmq = new PGMQClient(pool);
        }
        return pgmq;
    }

    @AfterAll
    static void afterAll() {
        if (postgres != null) {
            postgres.stop();
        }
    }

}
