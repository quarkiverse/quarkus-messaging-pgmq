/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package io.quarkiverse.messaging.pgmq.it;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.quarkiverse.messaging.pgmq.client.PGMQMessage;
import io.quarkiverse.messaging.pgmq.connector.PGMQIncomingMetadata;
import io.quarkiverse.messaging.pgmq.connector.PGMQOutgoingMetadata;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

@Path("/messaging-pgmq")
@ApplicationScoped
public class ReactiveMessagingPgmqResource {

    @Inject
    @Channel("test-out")
    Emitter<JsonObject> emitter;

    @Inject
    @Channel("archive-out")
    Emitter<JsonObject> archiveEmitter;

    @Inject
    @Channel("headers-out")
    Emitter<JsonObject> headersEmitter;

    @Inject
    @Channel("pojo-out")
    Emitter<TestMessage> pojoEmitter;

    private final List<JsonObject> received = new CopyOnWriteArrayList<>();
    private final List<JsonObject> archivedReceived = new CopyOnWriteArrayList<>();
    private final List<PGMQMessage> receivedMessages = new CopyOnWriteArrayList<>();
    private final List<TestMessage> pojoReceived = new CopyOnWriteArrayList<>();

    @Incoming("test-in")
    void consume(JsonObject msg) {
        received.add(msg);
    }

    @Incoming("archive-in")
    void consumeArchive(JsonObject msg) {
        archivedReceived.add(msg);
    }

    @Incoming("headers-in")
    void consumeWithMetadata(JsonObject msg, PGMQIncomingMetadata metadata) {
        receivedMessages.add(metadata.getMessage());
    }

    @Incoming("pojo-in")
    void consumePojo(TestMessage msg) {
        pojoReceived.add(msg);
    }

    @GET
    public String hello() {
        return "Hello messaging-pgmq";
    }

    @POST
    @Path("/send")
    @Consumes(MediaType.TEXT_PLAIN)
    public void send(String message) {
        emitter.send(new JsonObject(message));
    }

    @GET
    @Path("/received")
    @Produces(MediaType.APPLICATION_JSON)
    public String getReceived() {
        return new JsonArray(received).encode();
    }

    @POST
    @Path("/send-archive")
    @Consumes(MediaType.TEXT_PLAIN)
    public void sendArchive(String message) {
        archiveEmitter.send(new JsonObject(message));
    }

    @GET
    @Path("/archive-received")
    @Produces(MediaType.APPLICATION_JSON)
    public String getArchiveReceived() {
        return new JsonArray(archivedReceived).encode();
    }

    @POST
    @Path("/send-with-headers")
    @Consumes(MediaType.TEXT_PLAIN)
    public void sendWithHeaders(String message, @HeaderParam("X-PGMQ-Headers") String headersJson) {
        JsonObject body = new JsonObject(message);
        JsonObject headers = new JsonObject(headersJson);
        PGMQOutgoingMetadata metadata = PGMQOutgoingMetadata.builder()
                .withHeaders(headers)
                .build();
        headersEmitter.send(Message.of(body, Metadata.of(metadata)));
    }

    @GET
    @Path("/received-metadata")
    @Produces(MediaType.APPLICATION_JSON)
    public String getReceivedMetadata() {
        JsonArray result = new JsonArray();
        for (PGMQMessage m : receivedMessages) {
            JsonObject entry = new JsonObject();
            entry.put("msgId", m.msgId());
            entry.put("readCount", m.readCount());
            entry.put("headers", m.headers());
            result.add(entry);
        }
        return result.encode();
    }

    @POST
    @Path("/send-multiple")
    @Consumes(MediaType.TEXT_PLAIN)
    public void sendMultiple(String message, @QueryParam("count") int count) {
        for (int i = 0; i < count; i++) {
            JsonObject body = new JsonObject(message);
            body.put("index", i);
            emitter.send(body);
        }
    }

    @POST
    @Path("/send-pojo")
    @Consumes(MediaType.TEXT_PLAIN)
    public void sendPojo(String message) {
        pojoEmitter.send(new JsonObject(message).mapTo(TestMessage.class));
    }

    @GET
    @Path("/pojo-received")
    @Produces(MediaType.APPLICATION_JSON)
    public String getPojoReceived() {
        JsonArray result = new JsonArray();
        for (TestMessage m : pojoReceived) {
            result.add(JsonObject.mapFrom(m));
        }
        return result.encode();
    }
}
