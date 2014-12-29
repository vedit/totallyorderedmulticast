package com.isikun.firat.totallyorderedmulticast;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.MulticastSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.logging.Logger;

/**
 * Created by hexenoid on 12/29/14.
 */
public class Multicaster {
    private final static Logger LOGGER = Logger.getLogger("Multicaster");
    private volatile boolean running;

//    private volatile Object[][] socketList; //muhtemelen hashmap yapmak zorunda kalcan
    private volatile Hashtable<Integer, Object[]> socketList;
    private List<Integer> portList;

    public Multicaster(){
        socketList = new Hashtable<>();
        portList = TOMProcess.getInstance().getPorts();
        running = true;
        for(int port: portList){
            if(port != TOMProcess.getInstance().getPort()){
                socketList.put(port, createSocket(port));
            }
        }
    }

    public void multicast(TOMMessage message){
        TOMMessage response;
        TOMTimestamp.increment();
        HashMap<Integer, Boolean> ackList = TOMProcess.getAckList(message.getFromPort());
        TOMProcess.getInboundQueue().put(message, ackList);
        for (Map.Entry<Integer, Object[]> entry : socketList.entrySet()) {
            Integer key = entry.getKey();
            Object[] ioComponents = entry.getValue();
            PrintWriter tmpWriter = (PrintWriter) ioComponents[1];
            BufferedReader tmpReader = (BufferedReader) ioComponents[0];
            tmpWriter.println(TOMMessage.serialize(message));
            try {
                response = TOMMessage.deserialize(tmpReader.readLine());
                if(response.getAction() == TOMMessage.ACTION_ACK){
                    TOMProcess.getInboundQueue().get(message).put(response.getFromPort(), true);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public Object[] createSocket(int port){
        Socket clientSocket = null;
        PrintWriter socketOut = null;
        BufferedReader socketIn = null;
        String host = "127.0.0.1";
        try {
            //create socket and connect to the server
            clientSocket = new Socket(host, port);
            //will use socketOut to send text to the server over the socket
            socketOut = new PrintWriter(clientSocket.getOutputStream(), true);
            //will use socketIn to receive text from the server over the socket
            socketIn = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        } catch (UnknownHostException e) { //if serverName cannot be resolved to an address
            LOGGER.severe("Who is " + host + "?");
            e.printStackTrace();
        } catch (IOException e) { //put it back to queue
            LOGGER.warning("Cannot Connect to :" + port);
        }
        return new Object[]{socketIn, socketOut};
    }

    public boolean isRunning() {
        return running;
    }

    public void stopRunning() {
        this.running = false;
        for (Map.Entry<Integer, Object[]> entry : socketList.entrySet()) {
            Integer key = entry.getKey();
            Object[] ioComponents = entry.getValue();
            BufferedReader tmpReader = (BufferedReader) ioComponents[0];
            PrintWriter tmpWriter = (PrintWriter) ioComponents[1];
            try {
                tmpReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            tmpWriter.close();
        }
    }

}
