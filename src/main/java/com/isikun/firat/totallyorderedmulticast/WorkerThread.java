package com.isikun.firat.totallyorderedmulticast;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.logging.Logger;

/**
 * Created by hexenoid on 12/27/14.
 */
public class WorkerThread implements Runnable {

    private final Socket clientSocket;

    public WorkerThread(Socket s) {
        clientSocket = s;
    }
    private final static Logger LOGGER = Logger.getLogger("WorkerThread");

    public void run() {
        //taken from Server4SingleClient
        PrintWriter socketOut = null;
        BufferedReader socketIn = null;
        try {
            //will use socketOut to send text to the server over the socket
            socketOut = new PrintWriter(clientSocket.getOutputStream(), true);
            //will use socketIn to receive text from the server over the socket
            socketIn = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        } catch (IOException e) {
            LOGGER.severe("Cannot get I/O for the connection.");
            e.printStackTrace();
            System.exit(1);
        }

        TOMMessage request = null;
        TOMMessage response = null;
        try {
            request = TOMMessage.deserialize(socketIn.readLine());
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (request != null) {
            response = TOMMessage.processResponse(request);
        } else {
            LOGGER.warning("Request is null");
        }

        socketOut.println(TOMMessage.serialize(response));
        LOGGER.info(TOMProcess.getInstance().getPid() + " recieved from " + request.getFromPid() + " at port:" + clientSocket.getPort() + " localport:" + clientSocket.getLocalPort() + ": \n\t\"" + request + "\"");
        LOGGER.info(TOMProcess.getInstance().getPid() + " sent: \n\t\"" + response + "\"");

        //close all streams
        socketOut.close();
        try {
            socketIn.close();
            clientSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
