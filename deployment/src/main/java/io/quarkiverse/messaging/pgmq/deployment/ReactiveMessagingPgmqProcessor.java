package io.quarkiverse.messaging.pgmq.deployment;

import java.util.HashSet;
import java.util.Set;

import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.AnnotationTarget;
import org.jboss.jandex.DotName;
import org.jboss.jandex.IndexView;
import org.jboss.jandex.MethodInfo;
import org.jboss.jandex.ParameterizedType;
import org.jboss.jandex.Type;

import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;

class ReactiveMessagingPgmqProcessor {

    private static final String FEATURE = "messaging-pgmq";

    private static final DotName INCOMING = DotName.createSimple("org.eclipse.microprofile.reactive.messaging.Incoming");
    private static final DotName OUTGOING = DotName.createSimple("org.eclipse.microprofile.reactive.messaging.Outgoing");
    private static final DotName CHANNEL = DotName.createSimple("org.eclipse.microprofile.reactive.messaging.Channel");
    private static final DotName EMITTER = DotName.createSimple("org.eclipse.microprofile.reactive.messaging.Emitter");
    private static final DotName MUTINY_EMITTER = DotName.createSimple("io.smallrye.reactive.messaging.MutinyEmitter");
    private static final DotName MESSAGE = DotName.createSimple("org.eclipse.microprofile.reactive.messaging.Message");

    private static final Set<DotName> SKIP_TYPES = Set.of(
            DotName.createSimple("io.vertx.core.json.JsonObject"),
            DotName.createSimple("io.vertx.core.json.JsonArray"),
            DotName.createSimple("java.lang.String"),
            DotName.createSimple("java.lang.Object"),
            DotName.createSimple("java.lang.Void"),
            DotName.createSimple("void"),
            MESSAGE,
            DotName.createSimple("org.eclipse.microprofile.reactive.messaging.Metadata"),
            DotName.createSimple("io.quarkiverse.messaging.pgmq.client.PGMQMessage"),
            DotName.createSimple("io.quarkiverse.messaging.pgmq.client.SendMessage"),
            DotName.createSimple("io.quarkiverse.messaging.pgmq.connector.PGMQIncomingMetadata"),
            DotName.createSimple("io.quarkiverse.messaging.pgmq.connector.PgmqAcknowledgement"),
            EMITTER,
            MUTINY_EMITTER,
            DotName.createSimple("io.smallrye.mutiny.Multi"),
            DotName.createSimple("io.smallrye.mutiny.Uni"),
            DotName.createSimple("java.util.concurrent.CompletionStage"),
            DotName.createSimple("java.util.concurrent.CompletableFuture"));

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }

    @BuildStep
    void registerPayloadTypesForReflection(CombinedIndexBuildItem combinedIndex,
            BuildProducer<ReflectiveClassBuildItem> reflectiveClass) {
        IndexView index = combinedIndex.getIndex();
        Set<String> typesToRegister = new HashSet<>();

        // Scan @Incoming method parameters for payload types
        for (AnnotationInstance incoming : index.getAnnotations(INCOMING)) {
            if (incoming.target().kind() == AnnotationTarget.Kind.METHOD) {
                MethodInfo method = incoming.target().asMethod();
                for (Type paramType : method.parameterTypes()) {
                    collectPayloadType(paramType, index, typesToRegister);
                }
            }
        }

        // Scan @Outgoing method return types for payload types
        for (AnnotationInstance outgoing : index.getAnnotations(OUTGOING)) {
            if (outgoing.target().kind() == AnnotationTarget.Kind.METHOD) {
                MethodInfo method = outgoing.target().asMethod();
                collectPayloadType(method.returnType(), index, typesToRegister);
            }
        }

        // Scan @Channel Emitter<T> fields and parameters for payload types
        for (AnnotationInstance channel : index.getAnnotations(CHANNEL)) {
            if (channel.target().kind() == AnnotationTarget.Kind.FIELD) {
                collectEmitterPayloadType(channel.target().asField().type(), index, typesToRegister);
            } else if (channel.target().kind() == AnnotationTarget.Kind.METHOD_PARAMETER) {
                collectEmitterPayloadType(
                        channel.target().asMethodParameter().method()
                                .parameterType(channel.target().asMethodParameter().position()),
                        index, typesToRegister);
            }
        }

        for (String type : typesToRegister) {
            reflectiveClass.produce(
                    ReflectiveClassBuildItem.builder(type)
                            .constructors(true)
                            .methods(true)
                            .fields(true)
                            .build());
        }
    }

    private void collectEmitterPayloadType(Type type, IndexView index, Set<String> types) {
        if (type.kind() != Type.Kind.PARAMETERIZED_TYPE) {
            return;
        }
        ParameterizedType pt = type.asParameterizedType();
        if ((EMITTER.equals(pt.name()) || MUTINY_EMITTER.equals(pt.name()))
                && !pt.arguments().isEmpty()) {
            collectPayloadType(pt.arguments().get(0), index, types);
        }
    }

    private void collectPayloadType(Type type, IndexView index, Set<String> types) {
        if (type == null) {
            return;
        }
        switch (type.kind()) {
            case CLASS -> {
                DotName name = type.asClassType().name();
                addIfUserType(name, index, types);
            }
            case PARAMETERIZED_TYPE -> {
                ParameterizedType pt = type.asParameterizedType();
                if (SKIP_TYPES.contains(pt.name())) {
                    // Unwrap generic framework types like Message<T>, Multi<T>, etc.
                    for (Type arg : pt.arguments()) {
                        collectPayloadType(arg, index, types);
                    }
                } else {
                    addIfUserType(pt.name(), index, types);
                }
            }
            default -> {
                // ignore primitives, wildcards, etc.
            }
        }
    }

    private void addIfUserType(DotName name, IndexView index, Set<String> types) {
        if (SKIP_TYPES.contains(name) || name.toString().startsWith("java.")) {
            return;
        }
        if (index.getClassByName(name) != null) {
            types.add(name.toString());
        }
    }
}
