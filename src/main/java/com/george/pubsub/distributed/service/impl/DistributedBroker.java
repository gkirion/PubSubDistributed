package com.george.pubsub.distributed.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.george.pubsub.distributed.exceptions.DistributedNodeException;
import com.george.pubsub.distributed.exceptions.ThirorosException;
import com.george.pubsub.distributed.service.DistributedBrokerable;
import com.george.pubsub.distributed.util.DistributedBrokerResponse;
import com.george.pubsub.distributed.util.DistributedNodeTopics;
import com.george.pubsub.thiroros.util.ChordUtils;
import com.george.pubsub.thiroros.util.DistributedNode;
import com.george.pubsub.thiroros.util.ThirorosResponse;
import com.george.pubsub.util.RemoteAddress;
import com.george.pubsub.util.RemoteSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import pubsub.broker.Broker;
import pubsub.broker.Message;
import pubsub.broker.Receivable;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.NoSuchAlgorithmException;
import java.util.*;

@Service("distributedBroker")
public class DistributedBroker implements DistributedBrokerable {

    private static Logger logger = LoggerFactory.getLogger(DistributedBroker.class);

    private RemoteAddress thirorosAddress;
    private RemoteAddress nodeAddress;
    private InnerBroker broker;
    private ObjectMapper mapper;
    private int idFrom;
    private int idTo;
    @Value("${max-retries:3}")
    private int maxRetries;

    @Autowired
    public DistributedBroker(@Value("${server.ip}") String nodeIp, @Value("${server.port}") int nodePort) {
        this(nodeIp, nodePort, "localhost", 50000);
    }

    public DistributedBroker(String nodeIp, int nodePort, String thirorosIp, int thirorosPort) {
        nodeAddress = new RemoteAddress(nodeIp, nodePort);
        thirorosAddress = new RemoteAddress(thirorosIp, thirorosPort);
        mapper = new ObjectMapper();
        broker = new InnerBroker();
        logger.info("created new distributed broker {} with thiroros {}", nodeAddress, thirorosAddress);
    }

    @Override
    public synchronized DistributedBrokerResponse publish(Message message) {
        try {
            int id = ChordUtils.computeId(message.getTopic());
            if (checkRange(id)) {
                broker.publish(message);
                return DistributedBrokerResponse.OK;
            } else {
                return DistributedBrokerResponse.INVALID_ID;
            }
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            return DistributedBrokerResponse.SERVER_ERROR;
        }
    }

    @Override
    public DistributedBrokerResponse publish(String topic, String text) {
        Message message = new Message(topic, text);
        return publish(message);
    }

    @Override
    public synchronized DistributedBrokerResponse subscribe(String topic, RemoteAddress remoteSubscriber) {
        try {
            int id = ChordUtils.computeId(topic);
            if (checkRange(id)) {
                RemoteSubscriber subscriber = new RemoteSubscriber(remoteSubscriber);
                subscriber.setMapper(mapper);
                broker.subscribe(topic, subscriber);
                return DistributedBrokerResponse.OK;
            } else {
                return DistributedBrokerResponse.INVALID_ID;
            }
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            return DistributedBrokerResponse.SERVER_ERROR;
        }
    }

    @Override
    public synchronized DistributedNodeTopics getTopics(int id) {
        Map<String, Set<Receivable>> topics = new HashMap<>();
        DistributedNodeTopics distributedNodeTopics = new DistributedNodeTopics();
        distributedNodeTopics.setToId(idTo);
        distributedNodeTopics.setTopics(topics);
        if (!checkRange(id)) {
            distributedNodeTopics.setDistributedBrokerResponse(DistributedBrokerResponse.INVALID_ID);
            return distributedNodeTopics;
        }
        try {
            for (String topic : broker.getSubscribers().keySet()) {
                int topicId = ChordUtils.computeId(topic);
                if (topicId >= id) {
                    topics.put(topic, broker.getSubscribers().get(topic));
                }
            }
            idTo = id - 1;
            distributedNodeTopics.setDistributedBrokerResponse(DistributedBrokerResponse.OK);
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            distributedNodeTopics.setDistributedBrokerResponse(DistributedBrokerResponse.SERVER_ERROR);
            return distributedNodeTopics;
        }
        return distributedNodeTopics;
    }

    @Override
    public synchronized void setTopics(Map<String, Set<Receivable>> topics) {
        for (String topic : topics.keySet()) {
            broker.getSubscribers().put(topic, topics.get(topic));
        }
    }

    @PostConstruct
    private void register() throws ThirorosException, DistributedNodeException {
        logger.info("registering node {}", nodeAddress);

        Optional<ThirorosResponse> thirorosResponse = join();
        if (thirorosResponse.isEmpty()) {
            logger.info("no response from thiroros, aborting...");
            throw new ThirorosException(ThirorosException.ThirorosExceptions.THIROROS_UNAVAILABLE);
        }

        idFrom = thirorosResponse.get().getNodeId();
        DistributedNode previousNode = thirorosResponse.get().getPreviousNode();
        if (previousNode == null) {
            idTo = idFrom - 1;
            logger.info("no previous node");
        } else {
            Optional<DistributedNodeTopics> distributedNodeTopics = getTopics(previousNode);
            if (distributedNodeTopics.isEmpty()) {
                logger.info("no response from previous node, aborting...");
                throw new DistributedNodeException(DistributedNodeException.DistributedNodeExceptions.DISTRIBUTED_NODE_UNAVAILABLE);
            }
            idTo = distributedNodeTopics.get().getToId();
            Map<String, Set<Receivable>> topics = distributedNodeTopics.get().getTopics();
            setTopics(topics);
        }
        logger.info("registering completed for node {}", nodeAddress);
    }

    private Optional<ThirorosResponse> join() throws ThirorosException {
        DistributedNode distributedNode = new DistributedNode();
        distributedNode.setRemoteAddress(nodeAddress);
        logger.info("sending join request for node {} to thiroros", distributedNode);
        try {
            HttpClient httpClient = HttpClient.newHttpClient();
            HttpRequest httpRequest = HttpRequest.newBuilder(URI.create("http://" + thirorosAddress.getIp() + ":" + thirorosAddress.getPort() + "/join")).POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(distributedNode))).build();
            //HttpResponse response = HttpBuilder.get(thirorosAddress.getIp(), thirorosAddress.getPort(), "join").body(mapper.writeValueAsString(distributedNode)).send();
            HttpResponse<String> response  = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            ThirorosResponse thirorosResponse = mapper.readValue(response.body(), ThirorosResponse.class);
            ThirorosResponse.Response status = thirorosResponse.getThirorosResponse();
            logger.info("thiroros response {}", status);

            if (status == ThirorosResponse.Response.REJECTED_INVALID_ID) {
                throw new ThirorosException(ThirorosException.ThirorosExceptions.INVALID_ID);
            } else if (status == ThirorosResponse.Response.REJECTED_SMALL_RANGE) {
                throw new ThirorosException(ThirorosException.ThirorosExceptions.SMALL_RANGE);
            }
            return Optional.of(thirorosResponse);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }

    private Optional<DistributedNodeTopics> getTopics(DistributedNode previousNode) throws DistributedNodeException {
        logger.info("sending get topics request to previous node {}", previousNode);
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest httpRequest = HttpRequest.newBuilder(URI.create("http://" + previousNode.getRemoteAddress().getIp() +  ":" + previousNode.getRemoteAddress().getPort() + "/distributedBroker/getTopics?id=" + idFrom)).build();
        //HttpResponse response = HttpBuilder.get(previousNode.getRemoteAddress().getIp(), previousNode.getRemoteAddress().getPort(), "/distributedBroker/getTopics?id=" + idFrom).send();

        try {
            HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            DistributedNodeTopics distributedNodeTopics = mapper.readValue(response.body(), DistributedNodeTopics.class);
            DistributedBrokerResponse status = distributedNodeTopics.getDistributedBrokerResponse();
            logger.info("get topics response {}", status);

            if (status == DistributedBrokerResponse.INVALID_ID) {
                throw new DistributedNodeException(DistributedNodeException.DistributedNodeExceptions.INVALID_ID);
            } else if (status == DistributedBrokerResponse.SERVER_ERROR) {
                throw new DistributedNodeException(DistributedNodeException.DistributedNodeExceptions.SERVER_ERROR);
            }
            return Optional.of(distributedNodeTopics);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }

    private DistributedNode findPreviousNode(List<DistributedNode> distributedNodes) {
        int index = -1;
        DistributedNode previousNode;
        for (DistributedNode distributedNode : distributedNodes) {
            if (idFrom <= distributedNode.getId()) {
                break;
            }
            index++;
        }
        if (index == -1) {
            previousNode = distributedNodes.get(distributedNodes.size() - 1);
        } else {
            previousNode = distributedNodes.get(index);
        }
        return previousNode;
    }

    private void unregister() {}

    private boolean checkRange(int id) {
        if (idFrom < idTo) {
            if (id >= idFrom && id <= idTo) {
                return true;
            }
        }  else {
            if (id >= idFrom || id <= idTo) {
                return true;
            }
        }
        return false;
    }

    private class InnerBroker extends Broker {

        public Map<String, Set<Receivable>> getSubscribers() {
            return subscribers;
        }

    }

}
