import com.hbase.bean.BaseConfig;
import com.hbase.bean.HBaseClient;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestQuery {
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


    private void showResult(Result result){
        System.out.println("行号："+ Bytes.toString(result.getRow()));
        System.out.println(col_goods_name+"："+result.getValue(Bytes.toBytes(BaseConfig.FAMILY_GOODS), Bytes.toBytes(col_goods_name)));
        System.out.println(col_goods_price+"："+result.getValue(Bytes.toBytes(BaseConfig.FAMILY_GOODS), Bytes.toBytes(col_goods_price)));
        System.out.println(col_goods_nums+"："+result.getValue(Bytes.toBytes(BaseConfig.FAMILY_GOODS), Bytes.toBytes(col_goods_nums)));


    }
    /**
     * 查询单条记录
     */
    @Test
    public void singleGetDemo() throws Exception{
        TableName tableName = TableName.valueOf(BaseConfig.NAMESPACE,BaseConfig.TABLE_USER_ACTION);
        Table table = hBaseClient.getTable(tableName);
        Get get = new Get("D00001".getBytes());
        Result result = table.get(get);
        showResult(result);
    }
    /**
     * 查询多条记录
     *
     */
    @Test
    public void batchGetDemo() throws Exception{
        TableName tableName = TableName.valueOf(BaseConfig.NAMESPACE,BaseConfig.TABLE_USER_ACTION);
        Table table = hBaseClient.getTable(tableName);
        List<Get> list = new ArrayList<Get>();
        Get get = new Get("D00001".getBytes());
        list.add(get);
        Result[] result = table.get(list);
        for(Result res : result){
            showResult(res);
        }
    }



    /**
     * 范围查询
     */
    @Test
    public void scanDemo(){

    }

    /**
     * 分页查询
     */
    @Test
    public void pageFilterDemo(){

    }
}
