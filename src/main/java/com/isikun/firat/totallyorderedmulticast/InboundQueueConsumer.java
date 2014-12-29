package com.isikun.firat.totallyorderedmulticast;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.SortedMap;

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

}
