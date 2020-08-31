package com.george.pubsub.distributed.util;

import pubsub.broker.Receivable;

import java.util.Map;
import java.util.Set;

public class DistributedNodeTopics {

    private DistributedBrokerResponse distributedBrokerResponse;
    private Map<String, Set<Receivable>> topics;
    private int toId;

    public DistributedBrokerResponse getDistributedBrokerResponse() {
        return distributedBrokerResponse;
    }

    public void setDistributedBrokerResponse(DistributedBrokerResponse distributedBrokerResponse) {
        this.distributedBrokerResponse = distributedBrokerResponse;
    }

    public Map<String, Set<Receivable>> getTopics() {
        return topics;
    }

    public void setTopics(Map<String, Set<Receivable>> topics) {
        this.topics = topics;
    }

    public int getToId() {
        return toId;
    }

    public void setToId(int toId) {
        this.toId = toId;
    }

    @Override
    public String toString() {
        return "DistributedNodeTopics{" +
                "distributedBrokerResponse=" + distributedBrokerResponse +
                ", topics=" + topics +
                ", toId=" + toId +
                '}';
    }

}
