package com.lumi.bigdata.datax.plugin.writer.kudu.server;

/**
 * @description: TODO
 * @author: liulin
 * @date: 2019/12/5 5:20 下午
 * @version: 1.0
 */
public enum ErrorCode implements com.alibaba.datax.common.spi.ErrorCode {
    KUDU_ERROR_TABLE(1, "表异常"),
    KUDU_ERROR_DATABASE(2, "数据库异常"),
    KUDU_ERROR_SERVER(3, "kudu server url参数异常"),
    KUDU_ERROR_PRIMARY_KEY(4, "kudu 表主键获取异常"),
    KUDU_ERROR_CONF_COLUMNS(5, "kudu列配置异常"),
    ILLEGAL_VALUES_ERROR(6, "字段数量异常"),
    ;

    int code;
    String description;

    ErrorCode(int code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return null;
    }

    @Override
    public String getDescription() {
        return null;
    }
}
