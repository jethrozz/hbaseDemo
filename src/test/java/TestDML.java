import com.hbase.bean.BaseConfig;
import com.hbase.bean.HBaseClient;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * hbase client java api 对表 的删出和添加的demo
 */
public class TestDML {


    private String masterIP = "";
    private String slave1IP = "";
    private String slave2IP = "";
    private String zkPort = "2181";
    HBaseClient hBaseClient = null;
    final private String col_goods_name = "goods_name";
    final private String col_goods_price = "goods_price";
    final private String col_goods_nums = "goods_nums";

    @Before
    public void init(){
        String zkQuorum = masterIP+":"+zkPort+","+slave1IP+":"+zkPort+","+slave2IP+":"+zkPort;
        hBaseClient = new HBaseClient(zkQuorum);
    }

    /**
     * 增
     */
    @Test
    public void add() throws Exception{
        //设置要获取的表名
        TableName tableUserAction = TableName.valueOf(BaseConfig.NAMESPACE,BaseConfig.TABLE_USER_ACTION);
        //获取到要操作的表
        Table table = hBaseClient.getTable(tableUserAction);

        //封装要添加的数据
        //这里传的参数是行号，由于hbase存储的都是二进制文件，所以必须将我们的数据转化为二进制后进行存储
        Put put = new Put(Bytes.toBytes("goods0001"));
        //addColumn(arg1,arg2,arg3);
        //arg1:列族名
        //arg2:key
        //arg3:value
        put.addColumn(Bytes.toBytes(BaseConfig.FAMILY_GOODS),Bytes.toBytes(col_goods_name),Bytes.toBytes("牙膏"));
        put.addColumn(Bytes.toBytes(BaseConfig.FAMILY_GOODS),Bytes.toBytes(col_goods_price),Bytes.toBytes(20.98));
        put.addColumn(Bytes.toBytes(BaseConfig.FAMILY_GOODS),Bytes.toBytes(col_goods_nums),Bytes.toBytes(300));
        //单个insert
        table.put(put);
        //批量insert，将需要批量insert的数据放入list中，用list作为参数传入
//      List<Put> list = new ArrayList<Put>();
//      list.add(put);
//      table.put(list);
        table.close();
    }
    /**
     * 删
     * 删除一行
     */
    @Test
    public void deleteRow() throws Exception{
        //设置要获取的表名
        TableName tableUserAction = TableName.valueOf(BaseConfig.NAMESPACE,BaseConfig.TABLE_USER_ACTION);
        //获取到要操作的表
        Table table = hBaseClient.getTable(tableUserAction);
        Delete delete = new Delete(Bytes.toBytes("goods0001"));

        //删除一行
        table.delete(delete);

        //批量删除
//      List<Delete> list = new ArrayList<Delete>();
//      list.add(delete);
//      table.delete(list);

        table.close();
    }

    /**
     * 删
     * 删除一列
     */
    @Test
    public void deleteCol() throws Exception{
        //设置要获取的表名
        TableName tableUserAction = TableName.valueOf(BaseConfig.NAMESPACE,BaseConfig.TABLE_USER_ACTION);
        //获取到要操作的表
        Table table = hBaseClient.getTable(tableUserAction);
        Delete delete = new Delete(Bytes.toBytes("goods0001"));

        delete.addColumn(Bytes.toBytes(BaseConfig.FAMILY_GOODS), Bytes.toBytes(col_goods_nums));
        table.delete(delete);
    }

    /** 删除之后，老version的数据无法插入，需要进行一次compact;新version的数据可以插入
     * <p>
     * flush 'gp:test'
     * major_compact 'gp:test'
     */
    @Test
    public void flushAndCompact() throws IOException {
        Admin admin = hBaseClient.getAdmin();
        TableName tableUserAction = TableName.valueOf(BaseConfig.NAMESPACE,BaseConfig.TABLE_USER_ACTION);
        admin.flush(tableUserAction);
        admin.majorCompact(tableUserAction);
    }
}
