package com.shilia.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

/**
 * 
 * Title: HBaseUtil
 * Description: HBase������ 
 * Version:1.0.0
 * @author pancm
 * @date 2017��12��6��
 */
public class HBaseUtil {
	private final Logger logger = LoggerFactory.getLogger(HBaseUtil.class);
    /** hadoop ���� */
    private static Configuration conf = null;
    /** hbase ���� */
    private static Connection con = null;
    /** �Ự */
    private static Admin admin = null;

    private static String ip ="tcs04";
    private static String port ="2181";
    private static String port1 ="9001";
       
   // ��ʼ������
   static {
       // ��������ļ�����
       conf = HBaseConfiguration.create(); 
       // �������ò���
        conf.set("hbase.zookeeper.quorum", ip);
        conf.set("hbase.zookeeper.property.clientPort", port);  
        //���hbase�Ǽ�Ⱥ������������ 
        //���ip�Ͷ˿�����hadoop/mapred-site.xml�����ļ����õ�
        conf.set("hbase.master", ip+":"+port1); 
   }
        

    /**
     * ��ȡ����
     * 
     * @return
     */
    public synchronized  Connection getConnection() {
        try {
            if (null == con || con.isClosed()) {
                // ������Ӷ���
                con = ConnectionFactory.createConnection(conf);
            }
        } catch (IOException e) {
            logger.error("获取连接失败！");
            e.printStackTrace();
        }

        return con;
    }

    /**
     * ���ӹر�
     */
    public  void close() {
        try {
            if (admin != null) {
                admin.close();
            }
            if (con != null) {
                con.close();
            }
        } catch (IOException e) {
            logger.error("连接关闭失败");
            e.printStackTrace();
        }
    }

    /**
     * ������
     * 
     * @param tableName
     *            ����
     * @param columnFamily
     *            ����
     */
    public void creatTable(String tableName, String[] columnFamily) {
        if(null==tableName||tableName.length()==0){
            return;
        }
        if(null==columnFamily||columnFamily.length==0){
            return;
        }
        // ������������
        TableName tn = TableName.valueOf(tableName);
        // a.�ж����ݿ��Ƿ����
        try {
            // ��ȡ�Ự
            admin = getConnection().getAdmin();
            if (admin.tableExists(tn)) {
                logger.info(tableName + " 表存在，删除表....");
                // ��ʹ������Ϊ���ɱ༭
                admin.disableTable(tn);
                // ɾ����
                admin.deleteTable(tn);
                logger.info("表删除成功.....");
            }
            // ������ṹ����
            HTableDescriptor htd = new HTableDescriptor(tn);
            for (String str : columnFamily) {
                // ��������ṹ����
                HColumnDescriptor hcd = new HColumnDescriptor(str);
                htd.addFamily(hcd);
            }
            // ������
            admin.createTable(htd);
            System.out.println(tableName + " �����ɹ���");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close();
        }
    }
    
    

    
    

    /**
     * ���ݵ�����������
     * 
     * @param tableName
     *            ����
     * @param rowKey
     *            �н� (����)
     * @param family
     *            ����
     * @param qualifier
     *            ��
     * @param value
     *            �����ֵ
     * @return
     */
    public void insert(String tableName, String rowKey, String family,
            String qualifier, String value) {
        Table t = null;
        try {
            t = getConnection().getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier),
                    Bytes.toBytes(value));
            t.put(put);
            logger.info(tableName + " 修改成功!");
        } catch (IOException e) {
            logger.error(tableName + " 修改失败!");
            e.printStackTrace();
        } finally {
            close();
        }
    }

    /**
     * ����������������
     * 
     * @param tableName
     *            ����
     * @param list
     *            hbase������ 
     * @return
     */
    public void insertBatch(String tableName, List<?> list) {
        if (null == tableName ||tableName.length()==0) {
            return;
        }
        if( null == list || list.size() == 0){
            return;
        }
        Table t = null;
        Put put = null;
        JSONObject json = null;
        List<Put> puts = new ArrayList<Put>();
        try {
            t = getConnection().getTable(TableName.valueOf(tableName));
            for (int i = 0, j = list.size(); i < j; i++) {
                json = (JSONObject) list.get(i);
                put = new Put(Bytes.toBytes(json.getString("rowKey")));
                put.addColumn(Bytes.toBytes(json.getString("family")),
                        Bytes.toBytes(json.getString("qualifier")),
                        Bytes.toBytes(json.getString("value")));
                puts.add(put);
            }
            t.put(puts);
            logger.info(tableName + " 修改成功!");
        } catch (IOException e) {
            logger.error(tableName + " 修改失败!");
            e.printStackTrace();
        } finally {
            close();
        }
    }
    
    /**
     * ����ɾ�� 
     * @param tableName ����
     * @param rowKey    �н�
     * @return
     */
    public void delete(String tableName, String rowKey) {
        delete(tableName,rowKey,"","");
    }
    
    /**
     * ����ɾ�� 
     * @param tableName ����
     * @param rowKey    �н�
     * @param family    ����
     * @return
     */
    public void delete(String tableName, String rowKey, String family) {
        delete(tableName,rowKey,family,"");
    }
    
    /**
     * ����ɾ�� 
     * @param tableName ����
     * @param rowKey    �н�
     * @param family    ����
     * @param qualifier ��
     * @return
     */
    public void delete(String tableName, String rowKey, String family,
            String qualifier) {
        if (null == tableName ||tableName.length()==0) {
            return;
        }
        if( null == rowKey || rowKey.length() == 0){
            return;
        }
        Table t = null;
        try {
            t = getConnection().getTable(TableName.valueOf(tableName));
            Delete del = new Delete(Bytes.toBytes(rowKey));
            // ������岻Ϊ��
            if (null != family && family.length() > 0) {
                // ����в�Ϊ��
                if (null != qualifier && qualifier.length() > 0) {
                    del.addColumn(Bytes.toBytes(family),
                            Bytes.toBytes(qualifier));
                } else {
                    del.addFamily(Bytes.toBytes(family));
                }
            }      
            t.delete(del);    
            logger.info("删除成功:"+del);
        } catch (IOException e) {
            logger.error("删除失败!");
            e.printStackTrace();
        } finally {
          close();
        }
    }
    
    /**
     * ��ѯ�ñ��е���������
     * 
     * @param tableName
     *            ����
     */
    public List<Map<String,Object>> select(String tableName) {
        if(null==tableName||tableName.length()==0){
            return null;
        }
        Table t = null;
        List<Map<String,Object>> list=new ArrayList<Map<String,Object>>();
        try {
            t = getConnection().getTable(TableName.valueOf(tableName));
            // ��ȡ����
            Scan scan = new Scan();
            // �õ�ɨ��Ľ����
            ResultScanner rs = t.getScanner(scan);
            if (null == rs ) {
                return null;
            }
            for (Result result : rs) {
                // �õ���Ԫ�񼯺�
                List<Cell> cs = result.listCells();
                if (null == cs || cs.size() == 0) {
                    continue;
                }
                for (Cell cell : cs) {
                    Map<String,Object> map=new HashMap<String, Object>();
                    map.put("rowKey", Bytes.toString(CellUtil.cloneRow(cell)));// ȡ�н�
                    map.put("timestamp", cell.getTimestamp());// ȡ��ʱ���
                    map.put("family", Bytes.toString(CellUtil.cloneFamily(cell)));// ȡ������
                    map.put("qualifier", Bytes.toString(CellUtil.cloneQualifier(cell)));// ȡ����
                    map.put("value", Bytes.toString(CellUtil.cloneValue(cell)));// ȡ��ֵ
                    list.add(map);
                }
            }
            logger.info("查询数据:"+list);
        } catch (IOException e) {
            logger.info("查询成功!");
            e.printStackTrace();
        } finally {
            close();
        }
        return list;
    }
    
    
    /**
     * 返回默认namespace,"default"内的数据表名
     */
    public TableName[] getListTable() throws IOException{
    	admin = getConnection().getAdmin();
    	return admin.listTableNamesByNamespace("default");
    }

    /**
     * ���ݱ������н���ѯ
     * @param tableName
     * @param rowKey
     */
    public void select(String tableName, String rowKey) {
        select(tableName,rowKey,"","");
    }
    
    /**
     * ���ݱ������н��������ѯ
     * @param tableName
     * @param rowKey
     * @param family
     */
    public void select(String tableName, String rowKey, String family) {
        select(tableName,rowKey,family,"");
    }
    
    /**
     * ����������ϸ��ѯ
     * 
     * @param tableName
     *            ����
     * @param rowKey
     *            �н� (����)
     * @param family
     *            ����
     * @param qualifier
     *            ��
     */
    public void select(String tableName, String rowKey, String family,
            String qualifier) {
        Table t = null;
        List<Map<String,Object>> list=new ArrayList<Map<String,Object>>();
        try {
            t = getConnection().getTable(TableName.valueOf(tableName));
            // ͨ��HBase�е� get�����в�ѯ
            Get get = new Get(Bytes.toBytes(rowKey));
            // ������岻Ϊ��
            if (null != family && family.length() > 0) {
                // ����в�Ϊ��
                if (null != qualifier && qualifier.length() > 0) {
                    get.addColumn(Bytes.toBytes(family),
                            Bytes.toBytes(qualifier));
                } else {
                    get.addFamily(Bytes.toBytes(family));
                }
            }
            Result r = t.get(get);
            List<Cell> cs = r.listCells();
            if (null == cs || cs.size() == 0) {
                return;
            }
            for (Cell cell : cs) {
                Map<String,Object> map=new HashMap<String, Object>();
                map.put("rowKey", Bytes.toString(CellUtil.cloneRow(cell)));// ȡ�н�
                map.put("timestamp", cell.getTimestamp());// ȡ��ʱ���
                map.put("family", Bytes.toString(CellUtil.cloneFamily(cell)));// ȡ������
                map.put("qualifier", Bytes.toString(CellUtil.cloneQualifier(cell)));// ȡ����
                map.put("value", Bytes.toString(CellUtil.cloneValue(cell)));// ȡ��ֵ
                list.add(map);
            }
            logger.info("查询的数据:"+list);
        } catch (IOException e) {
            logger.info("查询失败!");
            e.printStackTrace();
        } finally {
            close();
        }
    }
}