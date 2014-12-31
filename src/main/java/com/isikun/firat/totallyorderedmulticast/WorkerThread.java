package com.isikun.firat.totallyorderedmulticast;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;

/**
 * Created by hexenoid on 12/27/14.
 */
public class WorkerThread implements Runnable {

    private final Socket clientSocket;
    private final static Logger LOGGER = Logger.getLogger("WorkerThread");
    private static PrintWriter socketOut;
    private static BufferedReader socketIn;
    final Queue<TOMMessage> queue;
    private boolean running;

    public WorkerThread(Socket socket) {
        running = true;
        clientSocket = socket;
        try {
            clientSocket.setSoTimeout(5000);
        } catch (SocketException e) {
            e.printStackTrace();
        }
        try {
            socketOut = new PrintWriter(clientSocket.getOutputStream(), true);
            socketIn = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        } catch (IOException e) {
            LOGGER.severe("Cannot get I/O for the connection.");
            e.printStackTrace();
        }
        queue = new ConcurrentLinkedQueue<>();
    }


    @Override
    public void run() {
        System.out.println("Worker spawned on " + clientSocket.getPort());
        while (running) {
            TOMMessage received = receive();
            if (received != null) {
                System.out.println(received);
                TOMMessage response = TOMMessage.processResponse(received);
                if (response != null) {
                    addToQueue(response);
                }
            }
            if (queue.peek() != null) {
                TOMMessage message = queue.poll();
                send(message);
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static TOMMessage receive() {
        TOMMessage result = null;
        try {
            result = TOMMessage.deserialize(socketIn.readLine());
        } catch (SocketTimeoutException e) {
            LOGGER.info("ReadTimeout");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static void send(TOMMessage message) {
        TOMTimestamp.increment();
        socketOut.println(TOMMessage.serialize(message));
    }

    public synchronized void addToQueue(TOMMessage message) {
        queue.offer(message);
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
        if (!running) {
            try {
                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
