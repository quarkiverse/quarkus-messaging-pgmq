package io.quarkiverse.messaging.pgmq.client;

import java.time.OffsetDateTime;
import java.util.Random;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.PoolOptions;

public class PGMQClientTestBase {

    private static final Random RANDOM = new Random();

    protected static PostgreSQLContainer postgres;

    protected static PGMQClient pgmq;

    private static Vertx vertx;

    protected String queueName;

    @BeforeAll
    static void beforeAll() {
        vertx = Vertx.vertx();
        postgres = pgmqContainer();
        postgres.start();
        pgmq = getClient();
    }

    private static PostgreSQLContainer pgmqContainer() {
        return new PostgreSQLContainer(
                DockerImageName.parse("ghcr.io/pgmq/pg18-pgmq:v1.11.0").asCompatibleSubstituteFor("postgres"))
                .withDatabaseName("testdb")
                .withUsername("postgres")
                .withPassword("postgres")
                .withInitScript("init-pgmq.sql");
    }

    @BeforeEach
    void setUp(TestInfo testInfo) {
        queueName = testInfo.getTestMethod().get().getName() + "_" + Integer.toHexString(RANDOM.nextInt());
    }

    public static synchronized PGMQClient getClient() {
        if (pgmq == null) {
            PgConnectOptions connectOptions = new PgConnectOptions()
                    .setCachePreparedStatements(true)
                    .setPort(postgres.getMappedPort(5432))
                    .setHost(postgres.getHost())
                    .setDatabase(postgres.getDatabaseName())
                    .setUser(postgres.getUsername())
                    .setPassword(postgres.getPassword());

            PoolOptions poolOptions = new PoolOptions().setMaxSize(5);
            Pool pool = Pool.pool(vertx, connectOptions, poolOptions);
            pgmq = new PGMQClient(pool);
        }
        return pgmq;
    }

    protected OffsetDateTime serverNow() {
        return pgmq.getClient().preparedQuery("SELECT now()")
                .execute()
                .map(rows -> rows.iterator().next().getOffsetDateTime(0))
                .await().indefinitely();
    }

    @AfterAll
    static void afterAll() {
        if (pgmq != null) {
            pgmq.getClient().close().await().indefinitely();
            pgmq = null;
        }
        if (postgres != null) {
            postgres.stop();
        }
    }

}
