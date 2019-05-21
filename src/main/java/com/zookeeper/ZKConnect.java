package com.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author abird
 * @date:2019-05-21
 * @Description:
 */
public class ZKConnect implements Watcher {
    private static final Logger log = LoggerFactory.getLogger(ZKConnect.class);

    private static final String ZKSERVER_PATH = "192.168.0.106:2181";

    //private static final String ZKSERVER_PATH = "192.168.0.106:2181,192.168.0.106:2182";

    private static final Integer timeout = 5000;

    @Override
    public void process(WatchedEvent event) {
        log.debug("接受到watch通知:{}",event);
    }

    public static void main(String[] args) throws Exception {
        ZooKeeper zk = new ZooKeeper(ZKSERVER_PATH, timeout, new ZKConnect());

        log.debug("客户端开始连接zookeeper服务器...");
        log.debug("连接状态:{}",zk.getState());

        Thread.sleep(2000);
        log.debug("连接状态:{}",zk.getState());
    }
}
