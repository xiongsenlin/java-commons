package org.xsl.common.test.db;

import org.junit.Test;
import org.xsl.common.util.BytesUtil;

/**
 * Created by xiongsenlin on 17/1/12.
 */
public class TestHbaseApi {

    @Test
    public void testGetRowKey() throws Exception {
        TestHbaseModel testHbaseModel = new TestHbaseModel();
        byte [] rowKey = testHbaseModel.getRowKey();
        System.out.println(BytesUtil.getStringFromBytes(rowKey));
    }

    @Test
    public void testGetPut() {

    }

}
