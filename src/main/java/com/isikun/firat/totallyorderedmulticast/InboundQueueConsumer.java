package com.isikun.firat.totallyorderedmulticast;

import com.google.gson.Gson;

import java.util.*;

/**
 * Created by hexenoid on 12/29/14.
 */
public class InboundQueueConsumer implements Runnable{
    final SortedMap<TOMMessage, HashMap<Integer, Boolean>> queue;

    public InboundQueueConsumer(SortedMap queue) {
        this.queue = queue;
    }

    @Override
    public void run() {
        while (true) {
            if (queue.firstKey() != null) {
                boolean deliverable = true;
                TOMMessage request = queue.firstKey();
                Collection<Boolean> acks = queue.get(request).values();
                for(Boolean ack: acks){
                    if(!ack){
                        deliverable = false;
                    }
                }
                if(deliverable){
                    queue.remove(request);
                    TOMProcess.processTransaction(request);
                }
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public String serialize(SortedMap queue){
        Gson gson = new Gson();
        String json = gson.toJson(queue);
        return json;
    }

    public String toString(){
        return serialize(queue);
//        for (Map.Entry<TOMMessage, HashMap<Integer, Boolean>> entry : queue.entrySet()) {
//            TOMMessage message = entry.getKey();
//            HashMap<Integer, Boolean> acklist = entry.getValue();
//            for (Map.Entry<Integer, Boolean> acks : acklist.entrySet()) {
//                Integer port = acks.getKey();
//                Boolean acklgfist = acks.getValue();
//            }
//
//        }

    }

}
