package com.george.pubsub.distributed.exceptions;

public class DistributedNodeException extends Exception {

    public DistributedNodeException(String msg) {
        super(msg);
    }

    public DistributedNodeException(DistributedNodeExceptions exception) {
        super(exception.name());
    }

    public enum DistributedNodeExceptions {
        DISTRIBUTED_NODE_UNAVAILABLE, INVALID_ID, SERVER_ERROR
    }

}
