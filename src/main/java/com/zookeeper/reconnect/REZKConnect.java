package com.zookeeper.reconnect;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author abird
 * @date:2019-05-21
 * @Description:zk回话重连机制
 */
public class REZKConnect implements Watcher {

    private static final Logger log = LoggerFactory.getLogger(REZKConnect.class);

    private static final String ZKSERVER_PATH = "192.168.0.106:2181";

    //private static final String ZKSERVER_PATH = "192.168.0.106:2181,192.168.0.106:2182";

    private static final Integer timeout = 5000;

    @Override
    public void process(WatchedEvent event) {
        log.debug("接受到watch通知:{}",event);
    }

    public static void main(String[] args) throws Exception {
        ZooKeeper zk = new ZooKeeper(ZKSERVER_PATH, timeout, new REZKConnect());

        long sessionId = zk.getSessionId();
        byte[] sessionPassword = zk.getSessionPasswd();

        log.warn("客户端开始连接zookeeper服务器...");
        log.warn("连接状态:{}",zk.getState());

        Thread.sleep(2000);
        log.warn("连接状态:{}",zk.getState());

        log.warn("开始回话重连...");
        ZooKeeper recon = new ZooKeeper(ZKSERVER_PATH, timeout, new REZKConnect(), sessionId, sessionPassword);
        log.warn("重新连接状态:{}",recon.getState());
        Thread.sleep(2000);
        log.warn("重新连接状态:{}",recon.getState());
    }
}
