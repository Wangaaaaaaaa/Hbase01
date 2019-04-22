package hbase;


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Hbase01 {
    private static Connection connection=null;
    public static void main(String[] args) {

    }

    static{
        HBaseConfiguration conf=new HBaseConfiguration();
        conf.set("hbase.zookeeper.quorum","192.168.200.131");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        try {
            connection= ConnectionFactory.createConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean exits(String table) throws Exception{
        Admin admin=connection.getAdmin();
       boolean b= admin.tableExists(TableName.valueOf(table));
        return b;
    }

    //创建表
    public static void createtable(String table,String...cf)throws Exception{
        Admin admin=connection.getAdmin();
        HTableDescriptor hTableDescriptor = new HTableDescriptor();
        admin.createTable(hTableDescriptor);
    }

    public static void delete(String table)throws Exception{
        Admin admin=connection.getAdmin();
        admin.disableTable(TableName.valueOf(table));
        admin.deleteTable(TableName.valueOf(table));
    }

    public static void addtable(String table,String rowkey,String columnfamily,String column,String value)throws Exception{
        Table table1 = connection.getTable(TableName.valueOf(table));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.add(Bytes.toBytes(columnfamily), Bytes.toBytes(column), Bytes.toBytes(value));
        table1.put(put);
        System.out.println("添加成功");
    }

    public static void show(String table)throws Exception{
        Table table1 = connection.getTable(TableName.valueOf(table));
        Scan scan=new Scan();
        ResultScanner results = table1.getScanner(scan);
        for (Result result : results){
            Cell[] cells = result.rawCells();
            for (Cell cell : cells ){
                System.out.println(cell.getFamilyArray().toString());
                System.out.println(cell.getQualifier().toString());
                System.out.println(cell.getValue().toString());
            }
        }
    }

    public static void get(String table)throws Exception{
        Table table1 = connection.getTable(TableName.valueOf(table));
        Get get = new Get(Bytes.toBytes(table));
        Result result = table1.get(get);
        Cell[] cells = result.rawCells();
            for (Cell cell : cells ){
                System.out.println(cell.getFamilyArray().toString());
                System.out.println(cell.getQualifier().toString());
                System.out.println(cell.getValue().toString());
            }

    }
}
