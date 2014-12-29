package com.isikun.firat.totallyorderedmulticast;

import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
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

    public static final int ACTION_INIT = 1;
    public static final int ACTION_ACK = 10;
    public static final int ACTION_TRANSACTION = 50;
    public static final int ACTION_ERROR = 666;

    private final static Logger LOGGER = Logger.getLogger("TOMMessage");


    public TOMMessage(int fromPid, int fromPort, int toPid, int toPort, int action) {
        this.fromPid = fromPid;
        this.toPid = toPid;
        this.fromPort = fromPort;
        this.toPort = toPort;
        this.action = action;
        this.timestamp = TOMTimestamp.getTime();
    }

    public TOMMessage(int fromPid, int fromPort, int toPid, int toPort, int action, String payLoad) {
        this.fromPid = fromPid;
        this.toPid = toPid;
        this.fromPort = fromPort;
        this.toPort = toPort;
        this.action = action;
        this.payLoad = payLoad;
        this.timestamp = TOMTimestamp.getTime();
    }

    public TOMMessage sendMessage() {
        TOMTimestamp.increment(); // increment clock
        Socket clientSocket = null;
        PrintWriter socketOut = null;
        BufferedReader socketIn = null;
        String host = "127.0.0.1";
//        LOGGER.info(request);
        try {
            //create socket and connect to the server
            clientSocket = new Socket(host, this.getToPort());
            //will use socketOut to send text to the server over the socket
            socketOut = new PrintWriter(clientSocket.getOutputStream(), true);
            //will use socketIn to receive text from the server over the socket
            socketIn = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        } catch (UnknownHostException e) { //if serverName cannot be resolved to an address
            LOGGER.severe("Who is " + host + "?");
            e.printStackTrace();
        } catch (IOException e) { //put it back to queue
            LOGGER.warning("Cannot Connect to " + this.getToPid() + ":" + this.getToPort());
            TOMProcess.getOutboundQueue().offer(this); //
        }
        TOMMessage response = null;
        if (socketOut != null && socketIn != null) {
            socketOut.println(TOMMessage.serialize(this));
            try {
                response = TOMMessage.deserialize(socketIn.readLine());
            } catch (IOException e) {
                e.printStackTrace();
            }
            socketOut.close();
            try {
                socketIn.close();
                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (socketIn == null) {
            LOGGER.severe("SocketOut is null");
        }
        if (socketOut == null) {
            LOGGER.severe("SocketIn is null");
        }
        return response;
    }

    public static TOMMessage processResponse(TOMMessage request) {
        TOMMessage response;
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
        return response;
    }

    public static TOMMessage initMessage(int toPid, int toPort) {
        return new TOMMessage(TOMProcess.getInstance().getPid(), TOMProcess.getInstance().getPort(), toPid, toPort, TOMMessage.ACTION_INIT);
    }

    public static TOMMessage ackMessage(int toPid, int toPort) {
        return new TOMMessage(TOMProcess.getInstance().getPid(), TOMProcess.getInstance().getPort(), toPid, toPort, TOMMessage.ACTION_ACK);
    }

    public static TOMMessage errorMessage(int toPid, int toPort) {
        return new TOMMessage(TOMProcess.getInstance().getPid(), TOMProcess.getInstance().getPort(), toPid, toPort, TOMMessage.ACTION_ERROR);
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

    @Override
    public int compareTo(TOMMessage o) {
        int result = this.getTimestamp() - o.getTimestamp();
        if(result == 0){
            result = this.getFromPid() - o.getFromPid();
        }
        return result;
    }
}
