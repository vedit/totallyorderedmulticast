package com.isikun.firat.totallyorderedmulticast;

import java.io.IOException;
import java.net.Socket;
import java.util.Queue;

/**
 * Created by hexenoid on 12/27/14.
 */
public class QueueConsumer implements Runnable {

    final Queue<TOMMessage> queue;

    public QueueConsumer(Queue queue) {
        this.queue = queue;
    }

    @Override
    public void run() {
        while (true) {
            if (queue.peek() != null) {
                if(TOMProcess.isServerUp(queue.peek().getToPort())){
                    TOMMessage message = queue.poll();
                    message.sendMessage();
                    System.out.println("!!!===SENT MESSAGE FROM QUEUE===!!!\n" + message);
                } else {
                    System.out.println("Waiting for PID " + queue.peek().getToPid() + " to open server on port " + queue.peek().getToPort());
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