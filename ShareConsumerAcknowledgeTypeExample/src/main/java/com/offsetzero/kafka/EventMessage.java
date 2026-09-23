package com.offsetzero.kafka;

public final class EventMessage {

    private String id;
    private String event;

    public EventMessage() {
    }

    public EventMessage(String id, String event) {
        this.id = id;
        this.event = event;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    @Override
    public String toString() {
        return "EventMessage{id='" + id + "', event='" + event + "'}";
    }
}