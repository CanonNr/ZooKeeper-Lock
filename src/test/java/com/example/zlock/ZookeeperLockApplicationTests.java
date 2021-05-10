package com.example.zlock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;

@SpringBootTest
class ZookeeperLockApplicationTests {

    @Resource
    ZooKeeper zooKeeper;


    @Test
    void contextLoads(){
        // 基于Zookeeper的watch机制和临时顺序节点实现分布式公平锁
        for (int i = 0; i < 100; i++) {
            new Thread(() -> {
                try {
                    // 加入一个新的客户端 等待获取锁
                    String s = zooKeeper.create("/baba/09/lock", "ok".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                    // 获取所有子节点
                    List<String> children = zooKeeper.getChildren("/baba/09", false);
                    // 将所有子节点加入到有序字典中
                    TreeSet<String> sortedNodes = new TreeSet<>(children);
                    if (s.equals("/baba/09/" + sortedNodes.first())) {
                        // 如果当前节点是第一个,则直接拿到锁
                        System.out.println(s+" :拿到锁...");
                        Thread.sleep(50); // 假装执行业务
                        // 业务结束 删除节点释放锁
                        zooKeeper.delete(s, -1);
                    } else {
                        // 没有拿到锁
                        SortedSet<String> strings = sortedNodes.headSet(s.replace("/baba/09/",""));
                        // watch 自己的上一位
                        String prePath = "/baba/09/"+strings.last();
                        CountDownLatch latch = new CountDownLatch(1);
                        System.out.println(prePath);
                        Stat exists = zooKeeper.exists(prePath, new LockWatcher(latch));
                        if(exists != null){
                            latch.await(); // 等待 watch 事件
                            System.out.println(s+" :拿到锁...");
                            Thread.sleep(50);  // 假装执行业务
                            // 执行业务结束 删除锁
                            zooKeeper.delete(s, -1);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }).start();
        }

        while (true){}

    }


    //添加 watcher 监听临时顺序节点的删除
    static class LockWatcher implements Watcher {
        private CountDownLatch latch = null;

        public LockWatcher(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void process(WatchedEvent event) {
            if(event.getType() == Event.EventType.NodeDeleted){
                latch.countDown();
            }
        }
    }

}
