package org.zk.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

public class App 
{
    public static void main( String[] args ) throws Exception{
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .connectString("192.168.217.128:2181,192.168.217.135:2181,192.168.217.136:2181").
                sessionTimeoutMs(4000)
                .retryPolicy(new ExponentialBackoffRetry(1000,3))
                 //重试策略每个一秒重试一次 总共从试3次
                 //如果制定了命名空间 namespace 则所有的动作都在该链接下, 这里可以用作区分不同项目组的数据
                .namespace("").build();
        curatorFramework.start();



        //创建监听
        PathChildrenCache cache=new PathChildrenCache(curatorFramework,"/top",true);
        cache.start();
        cache.rebuild();
        cache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework framwork, PathChildrenCacheEvent event) throws Exception {
                System.err.println("节点发生变化:"+event.getType());
            }
        });


        Stat stat = new Stat();
        //查询节点数据
        byte[] bytes = curatorFramework.getData().storingStatIn(stat).forPath("/hadoop");
        System.out.println(new String(bytes));


        System.out.println("准备创建【/top/ni】");
        curatorFramework.create().withMode(CreateMode.PERSISTENT)
                .forPath("/top/ni", "李小龙".getBytes());
        System.out.println("节点【/top/ni】创建成功");



        stat = curatorFramework.checkExists().forPath("/top/ni");
        if(stat!= null){
            System.out.println("【/top/ni】节点存在，直接删除");
            curatorFramework.delete().forPath("/top/ni");
        }


        byte[] bs=curatorFramework.getData().forPath("/hadoop");
        System.out.println("数据:"+new String(bs));

        curatorFramework.close();
    }
}
