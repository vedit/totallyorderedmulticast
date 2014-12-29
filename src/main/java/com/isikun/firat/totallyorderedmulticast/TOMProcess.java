package com.isikun.firat.totallyorderedmulticast;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.MulticastSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by hexenoid on 12/27/14.
 */
public class TOMProcess {
    private int pid;
    private int port;
    private int totalProcesses;
    private List<Integer> ports;

    private static InboundQueueConsumer inboundConsumer;
    public static volatile SortedMap<TOMMessage, HashMap<Integer, Boolean>> inboundQueue;
    private static QueueConsumer outboundConsumer;

    private static volatile ServerThread socketServer;

    private static volatile TOMTimestamp timestamp;
    private final static String propFileName = System.getProperty("configFile", "config0.properties");
    private final static Logger LOGGER = Logger.getLogger("TOMProcess");

    private static volatile TOMProcess instance = null;

    public static volatile int currentState;

    private static volatile int balance;

    public volatile Hashtable<Integer, Boolean> waitingForConnection;
    public volatile Hashtable<Integer, Boolean> shouldConnect;

    public static int STATE_INIT = 0;
    public static int STATE_OPERATIONAL = 1;
    public static int STATE_SHUTTING_DOWN = 2;

    private static Queue<TOMMessage> outboundQueue;

    public static TOMProcess getInstance() {
        if (instance == null) {
            synchronized (TOMProcess.class) {
                if (instance == null) {
                    instance = new TOMProcess();
                }
            }
        }
        return instance;
    }

    private TOMProcess() {
        balance = 100;
        LOGGER.setLevel(Level.INFO);
        currentState = STATE_INIT;
        LOGGER.info("Setting state to Init");
        loadConfig();
        shouldConnect = new Hashtable<>();
        waitingForConnection = new Hashtable<>();
        timestamp = TOMTimestamp.getInstance();
        outboundQueue = new ConcurrentLinkedQueue<TOMMessage>();
        inboundQueue = Collections.synchronizedSortedMap(new TreeMap<TOMMessage, HashMap<Integer, Boolean>>());
        synchronized (TOMProcess.class) {
            new java.util.Timer().schedule(
                    new java.util.TimerTask() {
                        @Override
                        public void run() {
                            inboundConsumer = new InboundQueueConsumer(inboundQueue);
                            new Thread(inboundConsumer).start();
                            outboundConsumer = new QueueConsumer(outboundQueue);
                            new Thread(outboundConsumer).start();
                            initSequence();
                        }
                    },
                    1000
            );
        }


    }

    private void loadConfig() {
        String propFileName = getPropFileName();
        ports = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(propFileName))) {
            int ctr = 0;
            for (String line; (line = br.readLine()) != null; ) {
                switch (ctr) {
                    case 0:
                        pid = Integer.parseInt(line);
                        ctr++;
                        break;
                    case 1:
                        totalProcesses = Integer.parseInt(line);
                        ctr++;
                        break;
                    default:
                        ports.add(Integer.parseInt(line));
                        ctr++;
                        break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        port = ports.get(pid);
        LOGGER.info("pid: " + pid + " port: " + port + " ports: " + ports);
    }

    private synchronized void initSequence() {
        for (int i = pid+1; i < totalProcesses; i++) {
            waitingForConnection.put(i, false);
        }
        for (int i = pid; i >= 0; i--) {
            shouldConnect.put(i, false);
        }
        waitingForConnection.remove(pid);
        shouldConnect.remove(pid);
        LOGGER.finest("WAITING FOR CONNECTIONS:");
        for (Map.Entry<Integer, Boolean> entry : waitingForConnection.entrySet()) {
            Integer key = entry.getKey();
            Boolean value = entry.getValue();
            LOGGER.finest(key + " " + value);
        }
        System.out.println("SHOULD CONNECT TO THESE NODES:");
        for (Map.Entry<Integer, Boolean> entry : shouldConnect.entrySet()) {
            Integer key = entry.getKey();
            Boolean value = entry.getValue();
            LOGGER.finest(key + " " + value);
        }
        if (pid == 0) {
            socketServer = ServerThread.getInstance();
        } else {
            for (int i = pid; i > 0; i--) {
                try{
                    int tempPid = i - 1;
                    int tempPort = ports.get(tempPid);
                    boolean isUp = false;
                    while(!isUp){
                        LOGGER.info("Waiting for PID " + tempPid + " to open server on port " + tempPort);
                        isUp = isServerUp(tempPort);
                        Thread.sleep(500);
                    }
                    TOMMessage response = TOMMessage.initMessage(tempPid, tempPort).sendMessage();
                    if (response.getAction() == TOMMessage.ACTION_ACK) {
                        shouldConnect.put(tempPid, true);
                        if (pid != (totalProcesses - 1)) {
                            socketServer = ServerThread.getInstance();
                            LOGGER.info("Starting server for init sequence " + pid);
                        }
                    }
                } catch (Exception e){
                    LOGGER.info("Waiting for PID  to open server on port ");
                }
            }
        }
        for (Map.Entry<Integer, Boolean> entry : waitingForConnection.entrySet()) {
            Integer key = entry.getKey();
            Boolean value = entry.getValue();
            while(!value){
                LOGGER.info("Waiting for "+ key + " to connect");
                value = waitingForConnection.get(key);
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        for (Boolean value: shouldConnect.values()) {
            if(!value) {
                LOGGER.severe("CRITICAL FAILURE!!111");
            }
        }
        LOGGER.info("INIT SEQUENCE COMPLETED");
        TOMProcess.currentState = STATE_OPERATIONAL;
        LOGGER.info("Setting state to Operational");
        if(socketServer != null){
            socketServer.setRunning(false);
        }
        System.out.println("Press 1 to generate event\n Press 2 to post event");
    }

    public static boolean isServerUp(int port) {
        try (Socket ignored = new Socket("localhost", port)) {
            return true;
        } catch (IOException ignored) {
            return false;
        }
    }

    public int getPid() {
        return pid;
    }

    public int getPort() {
        return port;
    }

    public static HashMap<Integer, Boolean> getAckList(){
        HashMap<Integer, Boolean> ackList = new HashMap<>();
        for(int port: TOMProcess.getInstance().getPorts()){
            if(TOMProcess.getInstance().getPort() == port){
                ackList.put(port, true);
            } else{
                ackList.put(port, false);
            }
        }
        return ackList;
    }

    public static HashMap<Integer, Boolean> getAckList(int fromPort){
        HashMap<Integer, Boolean> ackList = new HashMap<>();
        for(int port: TOMProcess.getInstance().getPorts()){
            if((TOMProcess.getInstance().getPort() == port) || (port == fromPort)){
                ackList.put(port, true);
            } else{
                ackList.put(port, false);
            }
        }
        return ackList;
    }

    public String getPropFileName() {
        return propFileName;
    }

    public static Queue getOutboundQueue(){
        return outboundQueue;
    }

    public static SortedMap<TOMMessage, HashMap<Integer, Boolean>> getInboundQueue() {
        return inboundQueue;
    }

    public static int getCurrentState() {
        return currentState;
    }

    public List<Integer> getPorts() {
        return ports;
    }

    public int getTotalProcesses() {
        return totalProcesses;
    }

    public static int getBalance() {
        return balance;
    }

    public static void setBalance(int balance) {
        TOMProcess.balance = balance;
    }

    public static boolean processTransaction(TOMMessage request) {
        setBalance(request.getPayLoad() + getBalance());
        LOGGER.info("New Balance: " + getBalance());
        return true;
    }
}
