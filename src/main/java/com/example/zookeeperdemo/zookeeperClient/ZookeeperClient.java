package com.example.zookeeperdemo.zookeeperClient;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.springframework.util.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZookeeperClient {

    // 客户端链接地址
    private static final String ZK_ADDRESS = "localhost:2181";

    // 客户端根节点
    private static final String ROOT_NODE = "/root";

    // 客户端子节点
    private static final String ROOT_NODE_CHILDREN = "/root/user";

    // 倒计数器
    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    private static ZooKeeper zooKeeper = null;

    public static void main(String[] args) throws Exception{

        initConnect(ZK_ADDRESS, 5000);
        createNode(ROOT_NODE, "root data");
        createNode(ROOT_NODE + "/home", "home data");

        createNodeRecursion(ROOT_NODE_CHILDREN, "recursion data");

        queryNode(ROOT_NODE);

        updateNodeData(ROOT_NODE, "good");


    }

    private static void initConnect(String connectAddress, int sessionTimeout) {
        try {
            zooKeeper = new ZooKeeper(connectAddress, sessionTimeout, watchedEvent -> {
                // 获取监听事件状态
                Watcher.Event.KeeperState state = watchedEvent.getState();
                // 获取监听事件类型
                Watcher.Event.EventType type = watchedEvent.getType();

                if (Watcher.Event.KeeperState.SyncConnected == state) {
                    if (Watcher.Event.EventType.None == type) {
                        System.out.println("Successful Connection!");
                        countDownLatch.countDown();
                    }
                }

                if (Watcher.Event.EventType.NodeCreated == type) {
                    System.out.println("Node Creation: " + watchedEvent.getPath());
                }
                if (Watcher.Event.EventType.NodeDataChanged == type) {
                    System.out.println("Node Update: " + watchedEvent.getPath());
                }
                if (Watcher.Event.EventType.NodeDeleted == type) {
                    System.out.println("Node Deletion: " + watchedEvent.getPath());
                }
                if (Watcher.Event.EventType.NodeChildrenChanged == type) {
                    System.out.println("Child Change" + watchedEvent.getPath());
                }
            });

            countDownLatch.await();
            System.out.println("Init Connection Success:" + zooKeeper);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void createNode(String nodePath, String data) throws KeeperException, InterruptedException {
        if (StringUtils.isEmpty(nodePath)) {
            System.out.println(nodePath + "can't be empty!");
            return;
        }

        Stat exists = zooKeeper.exists(nodePath, true);
        if (null != exists) {
            System.out.println(nodePath + "already exists!");
            return;
        }

        String result = zooKeeper.create(nodePath, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(nodePath + " --> " + data);
    }

    private static void createNodeRecursion(String nodePath, String data) throws KeeperException, InterruptedException {
        if (StringUtils.isEmpty(nodePath)) {
            System.out.println(nodePath + "can't be empty!");
        }

        String[] paths = nodePath.substring(1).split("/");
        for (int i = 0; i < paths.length; i++) {
            String childPath = "";
            for (int j = 0; j <= i; j++) {
                childPath += "/" + paths[j];
            }
            createNode(childPath, data);
        }
    }

    private static void queryNode(String nodePath) throws KeeperException, InterruptedException {
        System.out.println("------------------------ line ------------------------");

        byte[] bytes = zooKeeper.getData(nodePath, false, null);
        System.out.println(new String(bytes));

        Stat stat = new Stat();
        byte[] data = zooKeeper.getData(nodePath, true, stat);
        System.out.println("QueryNode: " + nodePath + " result: " + new String(data) +
                "stat: " + stat);
    }

    private static void updateNodeData(String nodePath, String data) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.setData(nodePath, data.getBytes(), -1);
        System.out.println("UpdateData: " + nodePath + stat);
    }

    private static void deleteNode(String nodePath) throws KeeperException, InterruptedException {
        System.out.println("------------------------ line ------------------------");

        Stat exists = zooKeeper.exists(nodePath, true);
        if (null == exists) {
            System.out.println(nodePath + " doesn't exist!");
            return;
        }

        zooKeeper.delete(nodePath, -1);
        System.out.println("DeleteNode " + nodePath);
    }

    private static void deleteNodeRecursion(String nodePath) throws KeeperException, InterruptedException {
        System.out.println("------------------------ line ------------------------");

        Stat exists = zooKeeper.exists(nodePath, true);
        if (null == exists) {
            System.out.println(nodePath + " doesn't exist!");
            return;
        }

        List<String> list = zooKeeper.getChildren(nodePath, true);
        if (CollectionUtils.isEmpty(list)) {
            deleteNode(nodePath);

            String parentPath = nodePath.substring(0, nodePath.lastIndexOf("/"));
            System.out.println("Parent Path: " + parentPath);

            if (StringUtils.isNotBlank(parentPath)) {
                deleteNodeRecursion(parentPath);
            }
        } else {
            for (String child : list) {
                deleteNodeRecursion(nodePath + "/" + child);
            }
        }
    }

}
