package com.isikun.firat.totallyorderedmulticast;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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

    private static volatile List<WorkerThread> workerThreads;

    private static volatile TOMTimestamp timestamp;
    private final static String propFileName = System.getProperty("configFile", "config0.properties");
    private final static Logger LOGGER = Logger.getLogger("TOMProcess");

    private static volatile TOMProcess instance = null;

    public static volatile int currentState;

    private static volatile double balance;

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
        workerThreads = new ArrayList<>();
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
                            terminalInterface();
                        }
                    },
                    1000
            );
        }


    }

    private synchronized void terminalInterface() {
        System.out.println("Supported actions are / * + -");
        System.out.println("Input one action and an integer for update. e.g. +2 /4");
        System.out.println("Input X to terminate");
        Scanner input = new Scanner(System.in);
        boolean running = true;
        while (running){
            String line = input.nextLine();
            if(line.contains("X")){
                running = false;
                currentState = TOMProcess.STATE_SHUTTING_DOWN;
                break;
            } else {
                try {
                    TOMProcess.multicast(TOMMessage.transactionMulticast(new TOMOperation(line).serialize()));
                } catch (InvalidPropertiesFormatException e) {
                    e.printStackTrace();
                }
            }
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
            LOGGER.info(key + " " + value);
        }
        System.out.println("SHOULD CONNECT TO THESE NODES:");
        for (Map.Entry<Integer, Boolean> entry : shouldConnect.entrySet()) {
            Integer key = entry.getKey();
            Boolean value = entry.getValue();
            LOGGER.info(key + " " + value);
        }
        if (pid == 0) {
            socketServer = new ServerThread(port);
            socketServer.startServer();
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
                    TOMMessage response = TOMMessage.initMessage(tempPid, tempPort).unicast(null);
                    if (response.getAction() == TOMMessage.ACTION_ACK) {
                        shouldConnect.put(tempPid, true);
                        if (pid != (totalProcesses - 1)) {
                            socketServer = new ServerThread(port);
                            socketServer.startServer();
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

    public static double getBalance() {
        return balance;
    }

    public static void setBalance(double balance) {
        TOMProcess.balance = balance;
    }

    public static boolean processTransaction(TOMMessage request) {
        TOMTimestamp.setTime(request.getTimestamp());
        TOMTimestamp.increment();
        setBalance(TOMOperation.deserialize(request.getPayLoad()).operate(getBalance()));
        LOGGER.info("New Balance: " + getBalance());
        return true;
    }



    public WorkerThread addWorker(Socket clientSocket){
        WorkerThread workerThread = new WorkerThread(clientSocket);
        workerThreads.add(workerThread);
        new Thread(workerThread).start();
        return workerThread;
    }

    public WorkerThread addWorker(int port){
        Socket clientSocket = null;
        try {
            clientSocket = new Socket("127.0.0.1", port);
        } catch (IOException e) {
            e.printStackTrace();
        }
        WorkerThread workerThread = new WorkerThread(clientSocket);
        workerThreads.add(workerThread);
        new Thread(workerThread).start();
        return workerThread;
    }

    public static List<WorkerThread> getWorkerThreads() {
        return workerThreads;
    }

    public static synchronized void multicast(TOMMessage message){
        for(WorkerThread workerThread: workerThreads){
            workerThread.addToQueue(message);
        }
    }
}
