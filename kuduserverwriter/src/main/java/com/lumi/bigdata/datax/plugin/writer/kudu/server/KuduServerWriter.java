package com.lumi.bigdata.datax.plugin.writer.kudu.server;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSONObject;

import com.lumi.bigdata.kudu.client.KuduDataTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;


/**
 * @description: TODO
 * @author: liulin
 * @date: 2019/12/5 3:37 下午
 * @version: 1.0
 */
public class KuduServerWriter extends Writer {

    public static final class Job extends Writer.Job {

        private static final Logger log = LoggerFactory.getLogger(Task.class);
        //提供多级JSON配置信息无损存储,采用json的形式
        private Configuration originalConfig;

        /**
         * init：Job对象初始化工作，测试可以通过super.getPluginJobConf()获取与本插件相关的配置
         */
        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
        }

        /**
         * split：拆分Task，参数adviceNumber框架建议的拆分数，一般运行时候所配置的并发度，值返回的是task的配置列表；
         *
         * @param mandatoryNumber 为了做到Reader、Writer任务数对等，这里要求Writer插件必须按照源端的切分数进行切分。否则框架报错！
         * @return List
         */
        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> list = new ArrayList<>();
            for (int i = 0; i < mandatoryNumber; i++) {
                //拷贝当前Configuration，注意，这里使用了深拷贝，避免冲突
                list.add(originalConfig.clone());
            }
            return list;
        }

        /**
         * destroy：Job对象自身的销毁工作
         */
        @Override
        public void destroy() {
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger log = LoggerFactory.getLogger(Task.class);

        private String dbName;
        private String tableName;
        private KuduDataTemplate kuduDataTemplate;
        private List<JSONObject> columnsList = new ArrayList<>();
        private List<JSONObject> primaryKeyList = new ArrayList<>();

        /**
         * init：Task对象的初始化，此时可以通过super.getPluginJobConf()获取与本Task相关的配置
         */
        @Override
        public void init() {
            //获取与本task相关的配置
            Configuration sliceConfig = super.getPluginJobConf();
            //判断表名是必须得参数
            tableName = sliceConfig.getNecessaryValue(Keys.KEY_KUDU_TABLE, ErrorCode.KUDU_ERROR_TABLE);
            dbName = sliceConfig.getNecessaryValue(Keys.KEY_KUDU_DATABASE, ErrorCode.KUDU_ERROR_DATABASE);
            //设置kudu服务器连接地址
            String serverAddress = sliceConfig.getNecessaryValue(Keys.KEY_KUDU_SERVER, ErrorCode.KUDU_ERROR_SERVER);
            //获取指定的primary key，可以指定多个
            primaryKeyList = sliceConfig.getList(Keys.KEY_KUDU_PRIMARY, JSONObject.class);
            //获取列名
            List<JSONObject> columnList = sliceConfig.getList(Keys.KEY_KUDU_COLUMN, JSONObject.class);
            if (primaryKeyList == null || primaryKeyList.isEmpty()) {
                throw DataXException.asDataXException(ErrorCode.KUDU_ERROR_PRIMARY_KEY, ErrorCode.KUDU_ERROR_PRIMARY_KEY.getDescription());
            }

            for (JSONObject column : columnList) {
                if (!column.containsKey("index")) {
                    throw DataXException.asDataXException(ErrorCode.KUDU_ERROR_CONF_COLUMNS, ErrorCode.KUDU_ERROR_CONF_COLUMNS.getDescription());
                } else if (!column.containsKey("name")) {
                    throw DataXException.asDataXException(ErrorCode.KUDU_ERROR_CONF_COLUMNS, ErrorCode.KUDU_ERROR_CONF_COLUMNS.getDescription());
                } else if (!column.containsKey("type")) {
                    throw DataXException.asDataXException(ErrorCode.KUDU_ERROR_CONF_COLUMNS, ErrorCode.KUDU_ERROR_CONF_COLUMNS.getDescription());
                } else {
                    columnsList.add(column.getInteger("index"), column);
                }
            }

            log.info("初始化KuduClient");
            kuduDataTemplate = new KuduDataTemplate(serverAddress);
        }

        /**
         * startWrite：从RecordReceiver中读取数据，写入目标数据源，RecordReceiver中的数据来自Reader和Writer之间的缓存队列。
         *
         * @param recordReceiver recordReceiver
         */
        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            Record record;
            List<Map<String, Object>> rowList = new LinkedList<>();
            AtomicLong counter = new AtomicLong(0);
            while ((record = recordReceiver.getFromReader()) != null) {
                if (columnsList.size() != record.getColumnNumber()) {
                    throw DataXException.asDataXException(ErrorCode.ILLEGAL_VALUES_ERROR, ErrorCode.ILLEGAL_VALUES_ERROR.getDescription() + "读出字段个数:" + record.getColumnNumber() + " " + "配置字段个数:" + columnsList.size());
                }
                boolean isDirtyRecord = false;
                for (int i = 0; i < primaryKeyList.size() && !isDirtyRecord; i++) {
                    JSONObject col = primaryKeyList.get(i);
                    Column column = record.getColumn(col.getInteger("index"));
                    isDirtyRecord = (column.getRawData() == null);
                }

                if (isDirtyRecord) {
                    super.getTaskPluginCollector().collectDirtyRecord(record, "primarykey字段为空");
                    continue;
                }

                Map<String, Object> row = new HashMap<>();
                for (JSONObject col : columnsList) {
                    String columnName = col.getString("name");
                    ColumnType columnType = ColumnType.valueOf(col.getString("type").toUpperCase());
                    Column column = record.getColumn(col.getInteger("index"));
                    Object data = column.getRawData();
                    if (data == null || data.toString().isEmpty()) {
                        continue;
                    }
                    switch (columnType) {
                        case INT:
                        case TINYINT:
                        case SMALLINT:
                            if (!data.toString().isEmpty()) {
                                row.put(columnName, Integer.parseInt(data.toString()));
                            }
                            break;
                        case BIGINT:
                            long putData;
                            if (data instanceof Date) {
                                putData = ((Date) data).toInstant().getEpochSecond();
                            } else {
                                putData = Long.parseLong(data.toString());
                            }
                            if (!String.valueOf(putData).isEmpty()) {
                                row.put(columnName, putData);
                            }
                            break;
                        case FLOAT:
                            row.put(columnName, Float.parseFloat(data.toString()));
                            break;
                        case STRING:
                        case CHAR:
                        case VARCHAR:
                            row.put(columnName, data.toString());
                            break;
                        case DOUBLE:
                            row.put(columnName, Double.parseDouble(data.toString()));
                            break;
                        case BOOLEAN:
                            row.put(columnName, Boolean.getBoolean(data.toString()));
                            break;
                    }
                }
                rowList.add(row);
                if (counter.incrementAndGet() > 95) {
                    try {
                        kuduDataTemplate.upsertRowList(dbName, tableName, rowList);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    rowList.clear();
                    counter.set(0);
                }
            }
            if (counter.get() > 0) {
                try {
                    kuduDataTemplate.upsertRowList(dbName, tableName, rowList);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        /**
         * destroy：Task对象自身的销毁工作
         */
        @Override
        public void destroy() {
            if (kuduDataTemplate != null) {
                kuduDataTemplate.close();
            }
        }
    }
}
