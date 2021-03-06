package com.george.pubsub.distributed.service;

import com.george.pubsub.distributed.util.DistributedBrokerResponse;
import com.george.pubsub.distributed.util.DistributedNodeTopics;
import com.george.pubsub.util.RemoteAddress;
import pubsub.broker.Message;
import pubsub.broker.Receivable;

import java.util.Map;
import java.util.Set;

public interface DistributedBrokerable {

    public DistributedBrokerResponse publish(Message message);
    public DistributedBrokerResponse publish(String topic, String text);
    public DistributedBrokerResponse subscribe(String topic, RemoteAddress remoteSubscriber);
    public DistributedNodeTopics getTopics(int id);
    public void setTopics(Map<String, Set<Receivable>> topics);

}