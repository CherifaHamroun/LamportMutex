package com.esi;


import java.io.*;
import java.net.Socket;
import java.util.*;

/**
 * Created by swair on 6/17/14.
 */
public class Node implements Runnable {
    private class NodeLookup {
        public HashMap<String,String> table;
        public NodeLookup(HashMap<String,String> table) {
            this.table = table;
        }

        public String getIP(int pid) {
            String ip_port = table.get(Integer.toString(pid));
            return ip_port.split(":")[0];
        }

        public int getPort(int pid) {
            String ip_port = table.get(Integer.toString(pid));
            return Integer.parseInt(ip_port.split(":")[1]);
        }
    }
    private LamportClock localclock;
    private LamportMutex mutex;
    private int pid;
    private int port;
    private boolean requested_crit = false;
    private NodeLookup lookup; // "pid" -> "ip:port"
    public final List<Integer> other_pids;
    private HashMap<Integer, Socket> chan_map;

    // data collection stuff
    private int total_application_msgs = 0;
    private int total_protocol_msgs = 0;

    private int count_per_crit = 0;
    private long delay_per_crit = 0;
    private PrintWriter log_writer;

    //total crit section executions - Stop after 20
    private int crit_executions = 0;

    public Node(int pid, String ConfigFile) {
        this.pid = pid;
        this.lookup = new NodeLookup(ConfigReader.getLookup(ConfigFile));
        //System.out.println("clock from config: "+ConfigReader.clocks.get(this.pid));
        this.port = lookup.getPort(pid);
        this.localclock = new LamportClock(ConfigReader.clocks.get(this.pid));
        this.other_pids = new ArrayList<Integer>();
        for(String i :lookup.table.keySet()) {
            int id = Integer.parseInt(i);
            if(id!=this.pid) {
                other_pids.add(id);
            }
        }
        this.mutex = new LamportMutex(this);
        String file_name = "node"+this.pid+".log";
        try {
            this.log_writer = new PrintWriter(file_name, "UTF-8");
            log_writer.println(String.format("%-12s %-12s","proto_msgs","delay_duration"));
        } catch(FileNotFoundException |UnsupportedEncodingException ex) {
            ex.printStackTrace();
        }
        this.chan_map = new HashMap<Integer,Socket>();
    }

    /*
    call init_connections before starting the main node thread,
    wait for 2 seconds on each exception and keep trying to
    establish all connections before going further
     */
    public void init_connections() {
        for(int pid: other_pids) {
            if(this.chan_map.containsKey(pid)) {
                continue;
            }
            String receiver_ip = lookup.getIP(pid);
            int receiver_port = lookup.getPort(pid);
            try (Socket sock = new Socket(receiver_ip, receiver_port)) {
                //OutputStream out = sock.getOutputStream();
                chan_map.put(pid,sock);
            } catch(IOException ex) {
                System.out.println("trying to establish connections with "+receiver_ip+":"+receiver_port);
                try {
                    Thread.sleep(1000);
                } catch(InterruptedException exp) {}
                init_connections();
            }
        }
    }
    public int getPid() { return this.pid; }
    public int getPort() { return this.port;}

    public void run_listener() {
        Thread listener = new NodeListenerThread(this);
        listener.start();
    }

    public synchronized void deliver_message(Message msg) {
        //System.out.println(msg+"  "+msg.getType()+".from..."+msg.getSender()+"   local clock: "+localclock.peek());
        this.localclock.msg_event(msg.getClock());
        //System.out.println("new local clock: "+localclock.peek());
        if      (msg.getType().equals("request")) {
            System.out.println(ConsoleColors.CYAN_BOLD + msg.getType().toUpperCase(Locale.ROOT) + ConsoleColors.RESET +" depuis "+msg);
            mutex.queue_request(msg);
            /*
            in version 1, send reply to all requests whatsoever
             */
            send_message(msg.getSender(),"reply");
        }
        else if (msg.getType().equals("release")) {
            System.out.println(ConsoleColors.BLUE_BOLD + msg.getType().toUpperCase(Locale.ROOT) +ConsoleColors.RESET+" depuis "+msg);
            //this.total_protocol_msgs += 1;
            mutex.release_request(msg);
        }
        else if (msg.getType().equals("reply")) {
            System.out.println(ConsoleColors.GREEN_BOLD + msg.getType().toUpperCase(Locale.ROOT) +ConsoleColors.RESET+" depuis "+msg);
            this.total_protocol_msgs += 1;
            mutex.reply_request(msg);
        }
    }

    public synchronized void send_message(int receiver, String type) {
        //System.out.println("sending message from:"+ pid+" to "+receiver);

        /*
        reply messages are never multicasts so
        we have to increase the local clock in send_message method
         */
        if(type.equals("reply")) {
            this.localclock.local_event();
        }

        //Data collection
        if (type.equals("application")) {
            this.total_application_msgs += 1;
        } else {
            if(receiver != this.pid) {
                /*
                the replies you send are not for YOUR
                crit execution requests.
                 */
                if (!type.equals("reply"))
                    this.total_protocol_msgs += 1;
            }
        }
        Message msg = new Message.MessageBuilder()
                .to(receiver)
                .from(this.pid)
                .clock(this.localclock.peek())
                .type(type).build();
        /*
        If sending message to self?
        type should better be "request"
         */
        if(receiver == this.pid && type.equals("request")) {
            this.mutex.queue_request(msg);
        }
        else {
            String receiver_ip = lookup.getIP(receiver);
            int receiver_port = lookup.getPort(receiver);

            try (Socket sock = new Socket(receiver_ip, receiver_port)) {
                OutputStream out = sock.getOutputStream();
                ObjectOutputStream outstream = new ObjectOutputStream(out);
                outstream.writeObject(msg);
                outstream.close();
                out.close();
                sock.close();

            } catch (IOException ex) {
                System.err.println("can't send message" + ex);
            }
        }
    }

    public synchronized void multicast(String type) {
        this.localclock.local_event();
        if(type.equals("request")) {
            send_message(this.pid, type);
        }
        for(String pid_str: lookup.table.keySet()) {
            int pid_int = Integer.parseInt(pid_str);
            if (pid_int == this.pid) {
                continue;
            }
            else {
                send_message(pid_int,type);
            }
        }
    }

    private synchronized void execute_crit() {
        write_sharedlog("Entering");
        System.out.println(ConsoleColors.RED_BACKGROUND + "<Execution de la SC par le site "+ this.pid +">"+ ConsoleColors.RESET );
        this.crit_executions += 1;

        try {
            Random r = new Random();
            int low = 1000;
            int high = 2000;
            int result = r.nextInt(high-low) + low;
            Thread.sleep(result);
        } catch(InterruptedException ex) {}
        write_sharedlog("Leaving");
    }

    @Override
    public void run() {
        run_listener();
        try {
            Thread.sleep(8000);//till I start other processes;
        } catch (InterruptedException ex) {}

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                cleanup();
            }
        });
        while(true) {
            Random rand = new Random();
            int sleeptime = rand.nextInt(5000 - 1000) + 1000;
            try {
                Thread.sleep(sleeptime);
            } catch (InterruptedException ex) {
                System.err.println(ex);
            }

            int decider = rand.nextInt(101 - 1) + 1;
            if (this.crit_executions >= 5) {
                System.out.println(ConsoleColors.RED_BOLD + "FIN des executions pour le site "+ this.pid +"!" + ConsoleColors.RESET);
            }
            else if (decider >= 1 && decider <= 90) {
                multicast("application");
            }
            else {
                long startTime = System.currentTimeMillis();
                int proto_messages_before = this.total_protocol_msgs;

                //multicast("request"); -- do that inside LamportMutex
                while(!mutex.request_crit_section()) {
                    //keep checking if we can enter crit or not.
                }
                long endTime = System.currentTimeMillis();
                long duration = (endTime - startTime);
                execute_crit();
                mutex.release_request();
                multicast("release");
                int proto_messages_after = this.total_protocol_msgs;
                this.delay_per_crit = duration;
                this.count_per_crit = proto_messages_after - proto_messages_before;
                log_and_reset();
            }
        }
    }

    public synchronized void log_and_reset() {
        log_writer.println(String.format("%-12s %-12s",this.count_per_crit,this.delay_per_crit));
        this.delay_per_crit = 0;
        this.count_per_crit = 0;
    }

    public synchronized void write_sharedlog(String action) {
        try {
            Writer sharedlog_writer = new PrintWriter(new FileWriter("shared.log",true));
            sharedlog_writer.append(
                    String.format("%-5s %-10s %-6s\n",this.pid,action,localclock.peek()));
            sharedlog_writer.close();
        } catch(IOException ex) {
            ex.printStackTrace();
        }
    }
    public void cleanup() {
        log_writer.println("total application messages sent: "+this.total_application_msgs);
        this.log_writer.close();
    }

    public static void main(String[] args) {
        //System.out.println(args[1]);
        Node n1 = new Node(1,"config.txt");
        Thread t1 = new Thread(n1);
        t1.start();
        Node n2 = new Node(2,"config.txt");
        Thread t2 = new Thread(n2);
        t2.start();
        Node n3 = new Node(3,"config.txt");
        Thread t3 = new Thread(n3);
        t3.start();
        Node n4 = new Node(4,"config.txt");
        Thread t4 = new Thread(n4);
        t4.start();
        Node n5 = new Node(5,"config.txt");
        Thread t5 = new Thread(n5);
        t5.start();
        Node n6 = new Node(6,"config.txt");
        Thread t6 = new Thread(n6);
        t6.start();
        Node n7 = new Node(7,"config.txt");
        Thread t7 = new Thread(n7);
        t7.start();
        Node n8 = new Node(8,"config.txt");
        Thread t8 = new Thread(n8);
        t8.start();
        Node n9 = new Node(9,"config.txt");
        Thread t9 = new Thread(n9);
        t9.start();
        Node n10 = new Node(10,"config.txt");
        Thread t10 = new Thread(n10);
        t10.start();
    }
}

