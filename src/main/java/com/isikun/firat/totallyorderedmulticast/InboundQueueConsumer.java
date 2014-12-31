package com.isikun.firat.totallyorderedmulticast;

import com.google.gson.Gson;

import java.util.*;

/**
 * Created by hexenoid on 12/29/14.
 */
public class InboundQueueConsumer implements Runnable{
    final SortedMap<TOMMessage, Boolean[]> queue;

    public InboundQueueConsumer(SortedMap queue) {
        this.queue = queue;
    }

    @Override
    public void run() {
        while (true) {
            if (!queue.isEmpty()) {
                boolean deliverable = true;
                TOMMessage request = queue.firstKey();
                for(Boolean ack: queue.get(request)){
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

    public synchronized void addToInboundQueue(TOMMessage message){
        Boolean[] tmpAcklist = new Boolean[TOMProcess.getInstance().getTotalProcesses()];
        for(int i=0; i<tmpAcklist.length; i++){
            tmpAcklist[i] = false;
        }
        queue.put(message, tmpAcklist);
    }

    public synchronized boolean ackMessage(TOMMessage ack){
        boolean result = false;

        for (Map.Entry<TOMMessage, Boolean[]> entry : queue.entrySet()) {
            TOMMessage message = entry.getKey();
            if(message.getUuid() == ack.getUuid()){
                Boolean[] acklist = entry.getValue();
                for(int i = 0; i < acklist.length; i++){
                    if(i == ack.getFromPid()){
                        result = true;
                        acklist[i] = true;
                    }
                }
                queue.put(message, acklist);
            }
        }
        return result;
    }

}
