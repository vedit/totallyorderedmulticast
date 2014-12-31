package com.isikun.firat.totallyorderedmulticast;

import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * Created by hexenoid on 12/27/14.
 */
public class TOMMessage implements Comparable<TOMMessage>{
    private int fromPid;
    private int toPid;
    private int fromPort;
    private int toPort;
    private int action;
    private int timestamp;
    private String payLoad;
    private boolean multicast;
    private UUID uuid;

    public static final int ACTION_INIT = 1;
    public static final int ACTION_ACK = 10;
    public static final int ACTION_TRANSACTION = 50;
    public static final int ACTION_ERROR = 666;

    private final static Logger LOGGER = Logger.getLogger("TOMMessage");


    public TOMMessage(int fromPid, int fromPort, int toPid, int toPort, int action, boolean multicast) {
        this.uuid = UUID.randomUUID();
        this.fromPid = fromPid;
        this.toPid = toPid;
        this.fromPort = fromPort;
        this.toPort = toPort;
        this.action = action;
        this.timestamp = TOMTimestamp.getTime();
        this.multicast = multicast;
    }

    public TOMMessage(int fromPid, int fromPort, int toPid, int toPort, int action, boolean multicast, String payLoad) {
        this.uuid = UUID.randomUUID();
        this.fromPid = fromPid;
        this.toPid = toPid;
        this.fromPort = fromPort;
        this.toPort = toPort;
        this.action = action;
        this.payLoad = payLoad;
        this.timestamp = TOMTimestamp.getTime();
        this.multicast = multicast;
    }

    public static TOMMessage unicast(TOMMessage message, Socket clientSocket){
        TOMTimestamp.setTime(message.getTimestamp());
        TOMTimestamp.increment();
        PrintWriter socketOut = null;
        BufferedReader socketIn = null;
        TOMMessage response = null;
        try {
            socketOut = new PrintWriter(clientSocket.getOutputStream(), true);
            socketIn = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        } catch (IOException e) {
            LOGGER.severe("Cannot get I/O for the connection.");
            e.printStackTrace();
        }
        if (socketOut != null) {
            System.out.println("Sent Message:");
            System.out.println(TOMMessage.serialize(message));
            socketOut.println(TOMMessage.serialize(message));
        }
        if (socketIn != null) {
            try {
                response = TOMMessage.deserialize(socketIn.readLine());
                System.out.println("Received Response:");
                System.out.println(response);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return response;
    }



    public TOMMessage unicast(Socket clientSocket){
        TOMMessage result = null;
        if(clientSocket == null) {
            try {
                clientSocket = new Socket("127.0.0.1", this.getToPort());
                WorkerThread worker = TOMProcess.getInstance().addWorker(clientSocket);
                result = unicast(this, clientSocket);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    public static TOMMessage processResponse(TOMMessage request) {
        TOMMessage response = null;
        if(request.isMulticast()){
            processMulticast(request);
        } else {
            switch (request.getAction()) {
                case TOMMessage.ACTION_INIT:
                    if(TOMProcess.getCurrentState() == TOMProcess.STATE_INIT){
                        TOMProcess.getInstance().waitingForConnection.put(request.fromPid, true);
                        response = ackMessage(request.fromPid, request.fromPort);
                    } else {
                        response = errorMessage(request.fromPid, request.fromPort);
                    }
                    break;
                case TOMMessage.ACTION_ACK:
                    response = ackMessage(request.fromPid, request.fromPort);
                    response = null;
                    break;
                case TOMMessage.ACTION_TRANSACTION:
                    TOMTimestamp.setTime(request.getTimestamp());
                    TOMProcess.getInboundQueue().put(request, TOMProcess.getAckList());
                    response = ackMessage(request.fromPid, request.fromPort);
                    break;
                default:
                    response = errorMessage(request.fromPid, request.fromPort);
                    break;
            }
        }

        return response;
    }

    public static void processMulticast(TOMMessage request) {
        TOMMessage response = null;
        switch (request.getAction()) {
            case TOMMessage.ACTION_ACK:

                response = null;
                break;
            case TOMMessage.ACTION_TRANSACTION:
                TOMTimestamp.setTime(request.getTimestamp());
                TOMProcess.getInboundQueue().put(request, TOMProcess.getAckList());
                response = ackMulticast(request.getUuid());
                break;
            default:
                response = null;
//                response = errorMessage(request.fromPid, request.fromPort);
                break;
        }
        if(response != null){
            TOMProcess.multicast(response);
        }
    }

    public static TOMMessage initMessage(int toPid, int toPort) {
        return new TOMMessage(TOMProcess.getInstance().getPid(), TOMProcess.getInstance().getPort(), toPid, toPort, TOMMessage.ACTION_INIT, false);
    }

    public static TOMMessage ackMessage(int toPid, int toPort) {
        return new TOMMessage(TOMProcess.getInstance().getPid(), TOMProcess.getInstance().getPort(), toPid, toPort, TOMMessage.ACTION_ACK, false);
    }

    public static TOMMessage errorMessage(int toPid, int toPort) {
        return new TOMMessage(TOMProcess.getInstance().getPid(), TOMProcess.getInstance().getPort(), toPid, toPort, TOMMessage.ACTION_ERROR, false);
    }

    public static TOMMessage transactionMulticast(String payLoad) {
        return new TOMMessage(TOMProcess.getInstance().getPid(), TOMProcess.getInstance().getPort(), TOMProcess.getInstance().getPid(), TOMProcess.getInstance().getPort(), TOMMessage.ACTION_TRANSACTION, true, payLoad);
    }
    public static TOMMessage ackMulticast(UUID uuid) {
        return new TOMMessage(TOMProcess.getInstance().getPid(), TOMProcess.getInstance().getPort(), TOMProcess.getInstance().getPid(), TOMProcess.getInstance().getPort(), TOMMessage.ACTION_ACK, true, uuid.toString());
    }

    public static String serialize(TOMMessage message) {
        Gson gson = new Gson();
        String json = gson.toJson(message);
        return json;
    }

    public static TOMMessage deserialize(String message) {
        Gson gson = new Gson();
        return gson.fromJson(message, TOMMessage.class);
    }

    public String toString() {
        return serialize(this);
    }

    public int getFromPid() {
        return fromPid;
    }

    public void setFromPid(int fromPid) {
        this.fromPid = fromPid;
    }

    public int getToPid() {
        return toPid;
    }

    public void setToPid(int toPid) {
        this.toPid = toPid;
    }

    public int getFromPort() {
        return fromPort;
    }

    public void setFromPort(int fromPort) {
        this.fromPort = fromPort;
    }

    public int getToPort() {
        return toPort;
    }

    public void setToPort(int toPort) {
        this.toPort = toPort;
    }

    public String getPayLoad() {
        return payLoad;
    }

    public void setPayLoad(String payLoad) {
        this.payLoad = payLoad;
    }

    public int getAction() {
        return action;
    }

    public void setAction(int action) {
        this.action = action;
    }

    public int getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }

    public boolean isMulticast() {
        return multicast;
    }

    public UUID getUuid() {
        return uuid;
    }

    @Override
    public int compareTo(TOMMessage o) {
        int result = this.getTimestamp() - o.getTimestamp();
        if(result == 0){
            result = this.getFromPid() - o.getFromPid();
        }
        return result;
    }
}
