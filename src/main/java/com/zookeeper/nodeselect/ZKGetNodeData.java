package com.zookeeper.nodeselect;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * @author abird
 * @date:2019-05-21
 * @Description:
 */
public class ZKGetNodeData implements Watcher {
    private static final Logger log = LoggerFactory.getLogger(ZKGetNodeData.class);
    private ZooKeeper zooKeeper = null;
    private static final String ZKSERVER_PATH = "192.168.0.106:2181";
    private static Stat stat = new Stat();

    private static CountDownLatch countDown = new CountDownLatch(1);

    //private static final String ZKSERVER_PATH = "192.168.0.106:2181,192.168.0.106:2182";

    private static final Integer timeout = 5000;

    public ZKGetNodeData() {
    }

    public ZKGetNodeData(String connectString) {
        try {
            zooKeeper = new ZooKeeper(connectString, timeout, new ZKGetNodeData());
        } catch (Exception e) {
            log.error("连接zookeeper失败:{}",e.getMessage());
            if (zooKeeper != null) {
                try {
                    zooKeeper.close();
                } catch (InterruptedException e1) {
                    log.error("关闭连接失败:{}",e1.getMessage());
                }
            }
        }
    }

    @Override
    public void process(WatchedEvent event) {

        try {
            if (event.getType() == Event.EventType.NodeDataChanged) {
                ZKGetNodeData zkServer = new ZKGetNodeData(ZKSERVER_PATH);
                byte[] resByte = zkServer.getZooKeeper().getData("/imooc", false, stat);
                String result = new String(resByte);
                System.out.println("修改后的值为:"+result);
                System.out.println("版本号变化dversion"+stat.getVersion());
                countDown.countDown();
            }
        } catch (KeeperException e) {

            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();

        }
    }

    public static void main(String[] args) throws KeeperException, InterruptedException {
        ZKGetNodeData zkServer = new ZKGetNodeData(ZKSERVER_PATH);

        byte[] resByte = zkServer.getZooKeeper().getData("/imooc", true, stat);
        String result = new String(resByte);
        System.out.println("当前值:"+ result);
        countDown.await();
    }

    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }
}
