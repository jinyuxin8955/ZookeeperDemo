package com.example.zookeeperdemo.curator;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.springframework.util.CollectionUtils;

import java.util.Collection;
import java.util.List;

public class CuratorClient {

    private static final String ZK_ADDRESS = "localhost:2181";

    private static final String ROOT_NODE = "/root";

    private static final String ROOT_NODE_CHILDREN = "/root/children";

    private final int SESSION_TIMEOUT = 20 * 1000;

    private final int CONNECTION_TIMEOUT = 5 * 1000;

    private static CuratorFramework client = null;

    private static final RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

    private static final RetryPolicy retry = new RetryNTimes(3, 2000);

    static {
        connectCuratorClient();
    }

    public static void connectCuratorClient() {
        client = CuratorFrameworkFactory.newClient(ZK_ADDRESS, retry);

        client.start();
        System.out.println("Successful Initialization: " +  client);
    }

    public static void main(String[] args) throws Exception {
        createNode(ROOT_NODE, null);
        createNode(ROOT_NODE_CHILDREN, "children data");

        getNode(ROOT_NODE);

        updateNode(ROOT_NODE, "update curator data");
        updateNode(ROOT_NODE_CHILDREN, "update curator data with Async");

//        deleteNode(ROOT_NODE);
    }

    private static void createNode(String nodePath, String data) throws Exception {
        if (StringUtils.isEmpty(nodePath)) {
            System.out.println(nodePath + " can't be empty!");
            return;
        }

        // 判断结点是否存在
        Stat exists = client.checkExists().forPath(nodePath);
        if (null != exists) {
            System.out.println(nodePath + "can't increase!");
            return;
        } else {
            System.out.println(StringUtils.join(nodePath + "can be created"));
        }

        if (StringUtils.isNotBlank(data)) {
            String node = client.create().forPath(nodePath, data.getBytes());
            System.out.println(node);

            client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(nodePath + "_sequential", data.getBytes());

            client.create().withMode(CreateMode.EPHEMERAL).forPath(nodePath + "/temp", data.getBytes());

            client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(nodePath + "/temp_sequential", data.getBytes());
        }
    }

    private static void getNode(String nodePath) throws Exception {
        byte[] bytes = client.getData().forPath(nodePath);
        System.out.println(StringUtils.join(nodePath, new String(bytes)));

        Stat stat = new Stat();
        byte[] byte2 = client.getData().storingStatIn(stat).forPath(nodePath);

        System.out.println(StringUtils.join(nodePath, new String(byte2)));
        System.out.println(stat);

        List<String> stringList = client.getChildren().forPath(nodePath);
        if (CollectionUtils.isEmpty(stringList)) {
            return;
        }
        stringList.forEach(System.out::println);
    }

    private static void updateNode(String nodePath, String data) throws Exception {
        Stat stat = client.setData().forPath(nodePath, data.getBytes());
        System.out.println(stat);

        // 异步设置结点数据
        Stat anotherStat = client.setData().inBackground().forPath(nodePath, data.getBytes());
        if (null != anotherStat) {
            System.out.println(anotherStat);
        }
    }

    private static void deleteNode(String nodePath) throws Exception {
        client.delete().deletingChildrenIfNeeded().forPath(nodePath);
    }

}
