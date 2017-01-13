package org.xsl.common.test.db;

import org.junit.Test;
import org.xsl.common.db.mysql.MysqlHelper;
import org.xsl.common.json.JsonHelper;
import org.xsl.common.config.Constants;
import org.xsl.common.config.Pair;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by xiongsenlin on 17/1/6.
 */
public class TestMysqlHelper {
    private MysqlHelper mysqlHelper;

    public TestMysqlHelper() {
        Map<String, String> conf = new HashMap<>();
        conf.put(Constants.MYSQL_CON_URL, "jdbc:mysql://127.0.0.1:3307/test?user=root&password=123456");

        this.mysqlHelper = new MysqlHelper(conf);
    }

    @Test
    public void testQuery() throws SQLException {
        String sql = "select * from test_user";

        List<Map<String, String>> result = this.mysqlHelper.query(sql);
        System.out.println(JsonHelper.toJson(result));

        List<List<Pair<String, String>>> result1 = this.mysqlHelper.queryInOrder(sql);
        System.out.println(JsonHelper.toJson(result1));

        List<Map<String, Object>> result2 = this.mysqlHelper.queryPrimitives(sql);
        System.out.println(JsonHelper.toJson(result2));
    }
}