package com.george.pubsub.distributed.exceptions;

public class ThirorosException extends Exception {

    public ThirorosException(String msg) {
        super(msg);
    }

    public ThirorosException(ThirorosExceptions exception) {
        super(exception.name());
    }

    public enum ThirorosExceptions {
        THIROROS_UNAVAILABLE, SMALL_RANGE, INVALID_ID, ALREADY_REGISTERED
    }

}
