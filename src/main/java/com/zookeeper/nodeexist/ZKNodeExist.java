package com.zookeeper.nodeexist;

import com.zookeeper.nodeselect.ZKGetNodeData;
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
 * @date:2019-05-22
 * @Description:判断zk节点是否存在
 */
public class ZKNodeExist implements Watcher {
    private static final Logger log = LoggerFactory.getLogger(ZKNodeExist.class);
    private ZooKeeper zooKeeper = null;
    private static final String ZKSERVER_PATH = "192.168.0.106:2181";

    private static CountDownLatch countDown = new CountDownLatch(1);

    //private static final String ZKSERVER_PATH = "192.168.0.106:2181,192.168.0.106:2182";

    private static final Integer timeout = 5000;

    public ZKNodeExist() {
    }

    public ZKNodeExist(String connectString) {
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
            if (event.getType() == Event.EventType.NodeCreated) {
                System.out.println("节点创建");
                countDown.countDown();
            } else if (event.getType() == Event.EventType.NodeDataChanged) {
                System.out.println("节点数据改变");
                countDown.countDown();
            } else if (event.getType() == Event.EventType.NodeDeleted) {

                System.out.println("节点删除");
                countDown.countDown();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws KeeperException, InterruptedException {
        ZKNodeExist zkServer = new ZKNodeExist(ZKSERVER_PATH);

        Stat stat = zkServer.getZooKeeper().exists("/imooc", true);
        if (stat != null) {
            log.warn("查询节点的版本dataVersion:{}", stat.getVersion());
        }else {
            log.warn("节点不存在");

        }
        countDown.await();
    }

    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }
}
