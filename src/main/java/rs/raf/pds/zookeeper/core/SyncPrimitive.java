package rs.raf.pds.zookeeper.core;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import java.io.IOException;

public class SyncPrimitive implements Watcher {

    protected ZooKeeper zk;
    protected Integer mutex;

    protected String root;

    protected SyncPrimitive(String address) {
        try {
            System.out.println("Starting ZK:");
            zk = new ZooKeeper(address, 3000, this);
            mutex = Integer.valueOf(-1);
            System.out.println("Finished starting ZK: " + zk);
        } catch (IOException e) {
            System.out.println(e.toString());
            zk = null;
        }
    }

    public void process(WatchedEvent event) {
        synchronized (mutex) {
            mutex.notify();
        }
    }


}
