package com.offsetzero.kafka;

public final class EventMessage {

    private String id;
    private String type;
    private String event;

    public EventMessage() {
    }

    public EventMessage(String id, String type, String event) {
        this.id = id;
        this.type = type;
        this.event = event;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    @Override
    public String toString() {
        return "EventMessage{id='" + id + "', type='" + type + "', event='" + event + "'}";
    }
}