package org.xsl.common.test.db;

import org.xsl.common.annotation.HbaseFieldAnnotation;
import org.xsl.common.db.hbase.HbaseModelBase;

/**
 * Created by xiongsenlin on 17/1/12.
 */
public class TestHbaseModel extends HbaseModelBase {
    @HbaseFieldAnnotation(rowKeyIndex = 0, isRowKeyField = true, isDataField = false)
    private String rowKey1 = "r1";

    @HbaseFieldAnnotation(rowKeyIndex = 1, isRowKeyField = true, isDataField = false)
    private String rowKey2 = "r2";

    @HbaseFieldAnnotation(mappingFiledName = "f1")
    private String data1 = "d1";

    @HbaseFieldAnnotation(mappingFiledName = "f2")
    private String data2 = "d2";

    @HbaseFieldAnnotation(mappingFiledName = "f3")
    private String data3 = "d3";

    public TestHbaseModel() {
        this.familyName = "f";
        this.rowKeySeparator = "_";
    }

    public String getRowKey1() {
        return rowKey1;
    }

    public void setRowKey1(String rowKey1) {
        this.rowKey1 = rowKey1;
    }

    public String getRowKey2() {
        return rowKey2;
    }

    public void setRowKey2(String rowKey2) {
        this.rowKey2 = rowKey2;
    }

    public String getData1() {
        return data1;
    }

    public void setData1(String data1) {
        this.data1 = data1;
    }

    public String getData2() {
        return data2;
    }

    public void setData2(String data2) {
        this.data2 = data2;
    }

    public String getData3() {
        return data3;
    }

    public void setData3(String data3) {
        this.data3 = data3;
    }
}
