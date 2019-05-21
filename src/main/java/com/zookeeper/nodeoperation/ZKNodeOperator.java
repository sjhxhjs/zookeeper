package com.zookeeper.nodeoperation;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author abird
 * @date:2019-05-21
 * @Description:zk节点操作
 */
public class ZKNodeOperator implements Watcher {
    private static final Logger log = LoggerFactory.getLogger(ZKNodeOperator.class);
    private ZooKeeper zooKeeper = null;
    private static final String ZKSERVER_PATH = "192.168.0.106:2181";

    //private static final String ZKSERVER_PATH = "192.168.0.106:2181,192.168.0.106:2182";

    private static final Integer timeout = 5000;

    public ZKNodeOperator() {
    }

    public ZKNodeOperator(String connectString) {
        try {
            zooKeeper = new ZooKeeper(connectString, timeout, new ZKNodeOperator());
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
    public void process(WatchedEvent watchedEvent) {

    }

    public static void main(String[] args) throws Exception {
        ZKNodeOperator zkServer = new ZKNodeOperator(ZKSERVER_PATH);

        //创建节点
        //zkServer.createZKNode("/testnode","testnode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE);

        //修改节点值
        /*Stat status = zkServer.getZooKeeper().setData("/testnode", "modify".getBytes(), 1);
        log.warn("修改节点值后的版本号:{}",status.getVersion());*/

        //删除节点
        zkServer.getZooKeeper().delete("/testnode",2);
    }

    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    /**
     * 创建节点
     * @param path
     * @param data
     * @param acls
     */
    private void createZKNode(String path, byte[] data, List<ACL> acls) {
        String result = "";

        try {
            result = zooKeeper.create(path, data, acls, CreateMode.PERSISTENT);
            log.warn("创建节点成功");
        } catch (Exception e) {
            log.error("创建节点失败:{}",e.getMessage());
        }
    }
}
