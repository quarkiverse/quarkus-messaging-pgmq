package io.quarkiverse.reactive.messaging.pgmq.deployment;

import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.FeatureBuildItem;

class ReactiveMessagingPgmqProcessor {

    private static final String FEATURE = "reactive-messaging-pgmq";

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }
}
