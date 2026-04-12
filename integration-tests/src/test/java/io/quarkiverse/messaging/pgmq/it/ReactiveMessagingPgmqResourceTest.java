package io.quarkiverse.messaging.pgmq.it;

import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;

@QuarkusTest
public class ReactiveMessagingPgmqResourceTest {

    @Test
    public void testHelloEndpoint() {
        given()
                .when().get("/messaging-pgmq")
                .then()
                .statusCode(200)
                .body(is("Hello messaging-pgmq"));
    }

    @Test
    public void testSendAndReceive() {
        String body = "{\"text\":\"hello-pgmq\"}";

        given()
                .contentType(ContentType.TEXT)
                .body(body)
                .when().post("/messaging-pgmq/send")
                .then()
                .statusCode(204);

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> given()
                .when().get("/messaging-pgmq/received")
                .then()
                .statusCode(200)
                .body("$.size()", is(org.hamcrest.Matchers.greaterThanOrEqualTo(1)))
                .body("[0].text", is("hello-pgmq")));
    }

    @Test
    public void testSendMultipleMessages() {
        String body = "{\"text\":\"multi\"}";

        given()
                .contentType(ContentType.TEXT)
                .body(body)
                .queryParam("count", 5)
                .when().post("/messaging-pgmq/send-multiple")
                .then()
                .statusCode(204);

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> given()
                .when().get("/messaging-pgmq/received")
                .then()
                .statusCode(200)
                .body("$.size()", is(org.hamcrest.Matchers.greaterThanOrEqualTo(6))));
    }

    @Test
    public void testAckArchive() {
        String body = "{\"text\":\"archive-test\"}";

        given()
                .contentType(ContentType.TEXT)
                .body(body)
                .when().post("/messaging-pgmq/send-archive")
                .then()
                .statusCode(204);

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> given()
                .when().get("/messaging-pgmq/archive-received")
                .then()
                .statusCode(200)
                .body("$.size()", is(org.hamcrest.Matchers.greaterThanOrEqualTo(1)))
                .body("[0].text", is("archive-test")));
    }

    @Test
    public void testSendAndReceiveWithHeaders() {
        String body = "{\"text\":\"headers-test\"}";
        String headers = "{\"correlation-id\":\"abc-123\",\"source\":\"integration-test\"}";

        given()
                .contentType(ContentType.TEXT)
                .header("X-PGMQ-Headers", headers)
                .body(body)
                .when().post("/messaging-pgmq/send-with-headers")
                .then()
                .statusCode(204);

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> given()
                .when().get("/messaging-pgmq/received-metadata")
                .then()
                .statusCode(200)
                .body("headers.correlation-id", hasItem("abc-123"))
                .body("headers.source", hasItem("integration-test")));
    }

    @Test
    public void testIncomingMetadata() {
        String body = "{\"text\":\"metadata-test\"}";
        String headers = "{\"trace\":\"meta\"}";

        given()
                .contentType(ContentType.TEXT)
                .header("X-PGMQ-Headers", headers)
                .body(body)
                .when().post("/messaging-pgmq/send-with-headers")
                .then()
                .statusCode(204);

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> given()
                .when().get("/messaging-pgmq/received-metadata")
                .then()
                .statusCode(200)
                .body("msgId", hasItem(org.hamcrest.Matchers.notNullValue()))
                .body("readCount", hasItem(org.hamcrest.Matchers.greaterThanOrEqualTo(1))));
    }

    @Test
    public void testPojoDeserialization() {
        String body = "{\"text\":\"pojo-test\",\"number\":42}";

        given()
                .contentType(ContentType.TEXT)
                .body(body)
                .when().post("/messaging-pgmq/send-pojo")
                .then()
                .statusCode(204);

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> given()
                .when().get("/messaging-pgmq/pojo-received")
                .then()
                .statusCode(200)
                .body("$.size()", is(org.hamcrest.Matchers.greaterThanOrEqualTo(1)))
                .body("[0].text", is("pojo-test"))
                .body("[0].number", is(42)));
    }

    @Test
    public void testHealthCheck() {
        given()
                .when().get("/q/health")
                .then()
                .statusCode(200)
                .body("status", is("UP"));
    }
}
