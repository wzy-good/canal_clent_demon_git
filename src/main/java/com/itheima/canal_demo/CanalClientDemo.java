package com.itheima.canal_demo;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClientDemo {
    public static void main(String[] args) {
        /**
         * 实现步骤：
         * 1：创建连接
         * 2：建立连接
         * 3：订阅主题
         * 4：获取数据
         * 5：递交确认
         * 6：关闭连接
         */
        //1创建连接
       // CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("node01", 11111), "example", "", "");
        //连接集群
        CanalConnector canalConnector = CanalConnectors.newClusterConnector("node01:2181", "example", "", "");

        //指定每次拉取的数据条数
        int batchSize = 1000;
        //定义一个标记循环获取数据
        boolean running = true;
        try{
           //2：建立连接
           canalConnector.connect();
           //回滚上次的get请求，重新获取数据
            canalConnector.rollback();
            //3：订阅主题，目的是订阅itcast_shop数据库中的所有表的更变
            canalConnector.subscribe("itcast_shop.*");
            //不停的拉取数据
            while(running){
                //4：获取数据,定义每次拉取的数据条数
                Message massage = canalConnector.getWithoutAck(batchSize);
                long batchId = massage.getId();
                //获取binlog的数据总数
                int size = massage.getEntries().size();
                if (size == 0 || size == -1){
                    //没有拉取到数据

                }else {
                    //拉取到了数据
                    printSummary(massage);
                }
                //递交消费位置
                canalConnector.ack(batchId);

            }

        }catch (Exception ex){
            ex.printStackTrace();

        }finally{
            //断开连接
            canalConnector.disconnect();

        }
    }

    private static void printSummary(Message message) {
        // 遍历整个batch中的每个binlog实体
        for (CanalEntry.Entry entry : message.getEntries()) {
            // 事务开始
            if(entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            // 获取binlog文件名
            String logfileName = entry.getHeader().getLogfileName();
            // 获取logfile的偏移量
            long logfileOffset = entry.getHeader().getLogfileOffset();
            // 获取sql语句执行时间戳
            long executeTime = entry.getHeader().getExecuteTime();
            // 获取数据库名
            String schemaName = entry.getHeader().getSchemaName();
            // 获取表名
            String tableName = entry.getHeader().getTableName();
            // 获取事件类型 insert/update/delete, 因为select查询不会打印操作日志，select不会改变数据
            //只有对数据的改变才会记录binlog日志
            String eventTypeName = entry.getHeader().getEventType().toString().toLowerCase();

            System.out.println("logfileName" + ":" + logfileName);
            System.out.println("logfileOffset" + ":" + logfileOffset);
            System.out.println("executeTime" + ":" + executeTime);
            System.out.println("schemaName" + ":" + schemaName);
            System.out.println("tableName" + ":" + tableName);
            System.out.println("eventTypeName" + ":" + eventTypeName);

            CanalEntry.RowChange rowChange = null;

            try {
                // 获取存储数据，并将二进制字节数据解析为RowChange实体
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }

            // 迭代每一条变更数据
            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                // 判断是否为删除事件
                if(entry.getHeader().getEventType() == CanalEntry.EventType.DELETE) {
                    System.out.println("---delete---");
                    printColumnList(rowData.getBeforeColumnsList());
                    System.out.println("---");
                }
                // 判断是否为更新事件
                else if(entry.getHeader().getEventType() == CanalEntry.EventType.UPDATE) {
                    System.out.println("---update---");
                    printColumnList(rowData.getBeforeColumnsList());
                    System.out.println("---");
                    printColumnList(rowData.getAfterColumnsList());
                }
                // 判断是否为插入事件
                else if(entry.getHeader().getEventType() == CanalEntry.EventType.INSERT) {
                    System.out.println("---insert---");
                    printColumnList(rowData.getAfterColumnsList());
                    System.out.println("---");
                }
            }
        }


    }

    // 打印所有列名和列值
    private static void printColumnList(List<CanalEntry.Column> columnList) {
        for (CanalEntry.Column column : columnList) {
            System.out.println(column.getName() + "\t" + column.getValue());
        }
    }
}
