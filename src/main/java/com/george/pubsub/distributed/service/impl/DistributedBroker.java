package com.george.pubsub.distributed.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.george.pubsub.distributed.service.DistributedBrokerable;
import com.george.pubsub.distributed.util.DistributedBrokerResponse;
import com.george.pubsub.distributed.util.DistributedNodeTopics;
import com.george.pubsub.thiroros.util.ChordUtils;
import com.george.pubsub.thiroros.util.DistributedNode;
import com.george.pubsub.thiroros.util.ThirorosResponse;
import com.george.pubsub.util.RemoteAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import pubsub.broker.Broker;
import pubsub.broker.Message;
import pubsub.broker.Receivable;

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
    int numberOfRetries = 0;
    int maxRetries = 3;

    @Autowired
    public DistributedBroker(@Value("${server.ip}") String nodeIp, @Value("${server.port}") int nodePort) {
        this("localhost", 50000, nodeIp, nodePort);
    }

    public DistributedBroker(String thirorosIp, int thirorosPort, String nodeIp, int nodePort) {
        thirorosAddress = new RemoteAddress();
        thirorosAddress.setIp(thirorosIp);
        thirorosAddress.setPort(thirorosPort);
        nodeAddress = new RemoteAddress();
        nodeAddress.setIp(nodeIp);
        nodeAddress.setPort(nodePort);
        mapper = new ObjectMapper();
        broker = new InnerBroker();
        logger.info("created new distributed broker {} with thiroros {}", nodeAddress, thirorosAddress);
        register();
    }

    @Override
    public synchronized DistributedBrokerResponse publish(Message message) {
        try {
            int id = ChordUtils.computeId(message.getTopic());
            if (checkRange(id)) {
                broker.publish(message);
                return DistributedBrokerResponse.OK;
            }
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return DistributedBrokerResponse.INVALIDATE_CACHE;
    }

    @Override
    public DistributedBrokerResponse publish(String topic, String text) {
        Message message = new Message(topic, text);
        return this.publish(message);
    }

    @Override
    public synchronized DistributedBrokerResponse subscribe(String topic, Receivable receivable) {
        try {
            int id = ChordUtils.computeId(topic);
            if (checkRange(id)) {
                broker.subscribe(topic, receivable);
                return DistributedBrokerResponse.OK;
            }
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return DistributedBrokerResponse.INVALIDATE_CACHE;
    }

    @Override
    public DistributedNodeTopics getTopics(int id) {
        return broker.getTopics(id);
    }

    @Override
    public void setTopics(Map<String, Set<Receivable>> topics) {
        broker.setTopics(topics);
    }

    public synchronized void register() {
        try {
            logger.info("registering node {}", nodeAddress);
            Optional<ThirorosResponse> thirorosResponse = join();
            if (thirorosResponse.isEmpty()) {
                logger.info("no response from thiroros, aborting...");
                return;
            }
            if (thirorosResponse.get().getThirorosResponse() != ThirorosResponse.Response.OK) {
                logger.info("thiroros response {} , aborting...", thirorosResponse.get().getThirorosResponse());
                return;
            }
            idFrom = thirorosResponse.get().getNodeId();
            DistributedNode previousNode = thirorosResponse.get().getPreviousNode();
            if (previousNode == null) {
                idTo = idFrom - 1;
                logger.info("no previous node");
                logger.info("registering completed for node {}", nodeAddress);
                return;
            }
            Optional<DistributedNodeTopics> distributedNodeTopics = getTopics(previousNode);
            if (distributedNodeTopics.isEmpty()) {
                logger.info("no response from previous node, aborting...");
                return;
            }
            if (distributedNodeTopics.get().getDistributedBrokerResponse() != DistributedBrokerResponse.OK) {
                logger.info("previous node response {}", distributedNodeTopics.get().getDistributedBrokerResponse());
                if (numberOfRetries < maxRetries) {
                    logger.info("retrying...");
                    numberOfRetries++;
                    register();
                } else {
                    logger.info("max retries reached, aborting...");
                    return;
                }
            }
            idTo = distributedNodeTopics.get().getToId();
            Map<String, Set<Receivable>> topics = distributedNodeTopics.get().getTopics();
            broker.setTopics(topics);
            logger.info("registering completed for node {}", nodeAddress);

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private Optional<ThirorosResponse> join() throws IOException, InterruptedException {
        DistributedNode distributedNode = new DistributedNode();
        distributedNode.setRemoteAddress(nodeAddress);
        logger.info("sending join request for node {} to thiroros", distributedNode);
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest httpRequest = HttpRequest.newBuilder(URI.create("http://" + thirorosAddress.getIp() + ":" + thirorosAddress.getPort() + "/join")).POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(distributedNode))).build();
        //HttpResponse response = HttpBuilder.get(thirorosAddress.getIp(), thirorosAddress.getPort(), "join").body(mapper.writeValueAsString(distributedNode)).send();
        HttpResponse<String> response = httpClient.send(httpRequest, java.net.http.HttpResponse.BodyHandlers.ofString());

        logger.info("thiroros response {}", response.body());
        if (response.statusCode() == 200) {
            return Optional.of(mapper.readValue(response.body(), ThirorosResponse.class));
        }
        return Optional.empty();
    }

    private Optional<DistributedNodeTopics> getTopics(DistributedNode previousNode) throws IOException, InterruptedException {
        logger.info("sending get topics request to previous node {}", previousNode);
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest httpRequest = HttpRequest.newBuilder(URI.create("http://" + previousNode.getRemoteAddress().getIp() +  ":" + previousNode.getRemoteAddress().getPort() + "/distributedBroker/getTopics?id=" + idFrom)).build();
        HttpResponse<String> response = httpClient.send(httpRequest, java.net.http.HttpResponse.BodyHandlers.ofString());
        //HttpResponse response = HttpBuilder.get(previousNode.getRemoteAddress().getIp(), previousNode.getRemoteAddress().getPort(), "/distributedBroker/getTopics?id=" + idFrom).send();
        logger.info("get topics response {}", response.body());
        if (response.statusCode() == 200) {
            return Optional.of(mapper.readValue(response.body(), DistributedNodeTopics.class));
        }
        return Optional.empty();
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

        public synchronized DistributedNodeTopics getTopics(int id) {
            Map<String, Set<Receivable>> topics = new HashMap<>();
            DistributedNodeTopics distributedNodeTopics = new DistributedNodeTopics();
            distributedNodeTopics.setToId(idTo);
            distributedNodeTopics.setTopics(topics);
            if (!checkRange(id)) {
                distributedNodeTopics.setDistributedBrokerResponse(DistributedBrokerResponse.INVALIDATE_CACHE);
                return distributedNodeTopics;
            }
            try {
                for (String topic : subscribers.keySet()) {
                    int topicId = ChordUtils.computeId(topic);
                    if (topicId >= id) {
                        topics.put(topic, subscribers.get(topic));
                    }
                }
                idTo = id - 1;
                distributedNodeTopics.setDistributedBrokerResponse(DistributedBrokerResponse.OK);
            } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            return distributedNodeTopics;
        }

        public synchronized void setTopics(Map<String, Set<Receivable>> topics) {
            for (String topic : topics.keySet()) {
                subscribers.put(topic, topics.get(topic));
            }
        }

    }

}