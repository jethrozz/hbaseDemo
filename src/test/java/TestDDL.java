import com.hbase.bean.BaseConfig;
import com.hbase.bean.HBaseClient;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;


public class TestDDL {
    private String masterIP = "";
    private String slave1IP = "";
    private String slave2IP = "";
    private String zkPort = "2181";
    HBaseClient hBaseClient = null;
    @Before
    public void init(){
        String zkQuorum = masterIP+":"+zkPort+","+slave1IP+":"+zkPort+","+slave2IP+":"+zkPort;
        hBaseClient = new HBaseClient(zkQuorum);
    }

    /**
     * 创建数据库（namespace）
     *
     */

    @Test
    public void  createNameSpace() throws Exception{
        Admin admin = hBaseClient.getAdmin();
        //hbase api没有类似existNamespace的方法，故以下变通的方法来判断某个namespace是否存在
        Set<String> existsNS = new HashSet<String>();
        //获取已存在的namespace名
        for (NamespaceDescriptor nd : admin.listNamespaceDescriptors()) {
            existsNS.add(nd.getName());
        }
        //删除
        if(existsNS.contains(BaseConfig.NAMESPACE)){
            System.out.println(BaseConfig.NAMESPACE + " exists,delete it first ...");
            //列出hbase中所有的表,逐个删除

            for(TableName tn : admin.listTableNames(BaseConfig.NAMESPACE)){
                //遍历该namespace下的所有表
                System.out.println("disable table:" + tn.getNameWithNamespaceInclAsString());
                //禁用该表
                admin.disableTable(tn);
                System.out.println("delete table:" + tn.getNameWithNamespaceInclAsString());
                //删除该表
                admin.deleteTable(tn);
            }
            //删除namespace
            admin.deleteNamespace(BaseConfig.NAMESPACE);
            System.out.println("delete ns:" + BaseConfig.NAMESPACE);
        }
        //创建ns
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(BaseConfig.NAMESPACE).build();
        admin.createNamespace(namespaceDescriptor);
        System.out.println("create namespace");

        admin.close();
    }

    /**
     * 创建表
     */
    @Test
    public void createTable() throws  Exception{
        //admin对象可以帮助我们完成DDL操作
        Admin admin = hBaseClient.getAdmin();
        //表名，指定在哪个namespace创建表
        TableName tableUserAction = TableName.valueOf(BaseConfig.NAMESPACE,BaseConfig.TABLE_USER_ACTION);
        HTableDescriptor tableUserActionDesciptor = new HTableDescriptor(tableUserAction);
        //列族
        HColumnDescriptor family_goods_info = new HColumnDescriptor(BaseConfig.FAMILY_GOODS);
        //设置最大版本号，最多支持5个版本
        family_goods_info.setMaxVersions(5);
        //设置存活时间，过期会自动删除
        family_goods_info.setTimeToLive(HConstants.FOREVER);
        HColumnDescriptor family_user_info = new HColumnDescriptor(BaseConfig.FAMILY_GOODS);
        //为该表添加列族
        tableUserActionDesciptor.addFamily(family_goods_info);
        tableUserActionDesciptor.addFamily(family_user_info);
        //建表
        admin.createTable(tableUserActionDesciptor);

        //关闭连接
        admin.close();
    }

    /**
     * 修改表
     */
    @Test
    public void updateTable() throws Exception{
        Admin admin = hBaseClient.getAdmin();
        //设置要获取的表名
        TableName tableUserAction = TableName.valueOf(BaseConfig.NAMESPACE,BaseConfig.TABLE_USER_ACTION);
        //删除列族
        admin.deleteColumn(tableUserAction,Bytes.toBytes(BaseConfig.FAMILY_USER));
        //关闭连接
        admin.close();
    }

    /**
     * 删除表
     * 已在创建数据库部分进行了演示
     */

}
