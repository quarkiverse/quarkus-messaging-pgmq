package io.quarkiverse.messaging.pgmq.it;

public class TestMessage {

    public String text;
    public int number;

    public TestMessage() {
    }

    public TestMessage(String text, int number) {
        this.text = text;
        this.number = number;
    }
}
