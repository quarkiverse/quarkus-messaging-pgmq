package io.quarkiverse.reactive.messaging.pgmq.it;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
public class ReactiveMessagingPgmqResourceTest {

    @Test
    public void testHelloEndpoint() {
        given()
                .when().get("/reactive-messaging-pgmq")
                .then()
                .statusCode(200)
                .body(is("Hello reactive-messaging-pgmq"));
    }
}
