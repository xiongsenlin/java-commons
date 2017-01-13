package org.xsl.common.test.db;

import org.junit.Test;
import org.xsl.common.db.mysql.MysqlCrudBase;
import org.xsl.common.db.mysql.MysqlHelper;
import org.xsl.common.config.Constants;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * Created by xiongsenlin on 17/1/11.
 */
public class TestMysqlCRUD {
    private MysqlHelper mysqlHelper;
    private TestTableCRUD testTableCRUD;

    public TestMysqlCRUD() {
        Map<String, String> conf = new HashMap<>();
        conf.put(Constants.MYSQL_CON_URL, "jdbc:mysql://127.0.0.1:3307/test?user=root&password=123456");

        this.mysqlHelper = new MysqlHelper(conf);
        this.testTableCRUD = new TestTableCRUD();
    }

    @Test
    public void testInsert() throws Exception {
        TestTableModel testTableModel1 = new TestTableModel();
        testTableModel1.setUserId(1);
        testTableModel1.setUsername("name1");
        testTableModel1.setNickname("nick1");
        this.testTableCRUD.insert(testTableModel1);

        TestTableModel testTableModel2 = new TestTableModel();
        testTableModel2.setUserId(2);
        testTableModel2.setUsername("name2");
        testTableModel2.setNickname("nick2");
        this.testTableCRUD.insert(testTableModel2);

        TestTableModel testTableModel3 = new TestTableModel();
        testTableModel3.setUserId(3);
        testTableModel3.setUsername("name3");
        testTableModel3.setNickname("nick3");
        this.testTableCRUD.insert(testTableModel3);
    }

    @Test
    public void testQuery() throws Exception {
        TestTableCRUD testTableCRUD = new TestTableCRUD();

        List<TestTableModel> models = testTableCRUD.query("select * from test_user");
        for (TestTableModel model : models) {
            System.out.println(model.toString());
        }

        assertTrue(models.size() == 3);
    }

    @Test
    public void testUpdate() throws Exception {
        TestTableCRUD testTableCRUD = new TestTableCRUD();

        List<TestTableModel> models = testTableCRUD.query("select * from test_user");
        for (TestTableModel model : models) {
            model.setGmtCreate(new Date());
            model.setGmtModify(new Date());

            testTableCRUD.update(model);
        }
    }

    @Test
    public void testDelete() throws Exception {
        List<TestTableModel> models = testTableCRUD.query("select * from test_user");
        for (TestTableModel model : models) {
            testTableCRUD.delete(model);
        }
    }

    private class TestTableCRUD extends MysqlCrudBase<TestTableModel> {
        public TestTableCRUD() {
            super(mysqlHelper);
        }
    }
}
