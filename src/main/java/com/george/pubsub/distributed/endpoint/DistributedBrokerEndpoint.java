package com.george.pubsub.distributed.endpoint;

import com.george.pubsub.distributed.service.DistributedBrokerable;
import com.george.pubsub.distributed.util.DistributedBrokerResponse;
import com.george.pubsub.distributed.util.DistributedNodeTopics;
import com.george.pubsub.util.RemoteSubscriber;
import com.george.pubsub.util.RemoteSubscription;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import pubsub.broker.Message;

@RestController
@RequestMapping("/distributedBroker")
public class DistributedBrokerEndpoint {

    @Autowired
    private DistributedBrokerable distributedBrokerable;

    @PostMapping("/publish")
    public DistributedBrokerResponse publish(@RequestBody Message message) {
        return distributedBrokerable.publish(message);
    }

    @PostMapping("/subscribe")
    public DistributedBrokerResponse subscribe(@RequestBody RemoteSubscription subscriber) {
        RemoteSubscriber remoteSubscriber = new RemoteSubscriber(subscriber.getRemoteSubscriber());
        return distributedBrokerable.subscribe(subscriber.getTopic(), remoteSubscriber);
    }

    @GetMapping("/getTopics")
    public DistributedNodeTopics getTopics(@RequestParam("id") int id) {
        return distributedBrokerable.getTopics(id);
    }

}
