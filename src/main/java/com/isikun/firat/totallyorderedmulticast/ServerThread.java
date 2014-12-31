package com.isikun.firat.totallyorderedmulticast;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.logging.Logger;

/**
 * Created by hexenoid on 12/27/14.
 */
public class ServerThread implements Runnable{

    private int port;

    private boolean running;


    private final static Logger LOGGER = Logger.getLogger("ServerThread");

    public ServerThread(int port) {
        this.running = true;
        this.port = port;
    }

    public void startServer() {
        ServerSocket serverSocket = null;

        try {
            //initialize server socket
            serverSocket = new ServerSocket(port);
            LOGGER.info("Server socket initialized at " + port + ".\n");
        } catch (IOException e) { //if this port is busy, an IOException is fired
            LOGGER.severe("Cannot listen on port " + port);
            e.printStackTrace();
            System.exit(0);
        }

        Socket clientSocket = null;
        try {
            while (running) { //infinite loop - terminate manually
                //wait for client connections
                LOGGER.info("Waiting for a client connection.");
                try {
                    clientSocket = serverSocket.accept();
                    TOMProcess.getInstance().addWorker(clientSocket);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(1);
                }
                String clientName = clientSocket.getInetAddress().getHostName();
                LOGGER.info(clientName + " established a connection.\n");
            }
        } finally {
            //make sure that the socket is closed upon termination
            try {
                serverSocket.close();
                LOGGER.warning("Shutting down server socket");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    @Override
    public void run() {
        startServer();
    }
}
