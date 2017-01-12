package org.xsl.common.test.db;

import org.xsl.common.annotation.MysqlFieldAnnotation;
import org.xsl.common.db.MysqlModelBase;
import java.util.Date;

public class TestTableModel extends MysqlModelBase {
    public TestTableModel() {
        super("test_user");
    }

    @MysqlFieldAnnotation(toDB = false, primaryKey = true)
    private Integer id = 0;

    @MysqlFieldAnnotation(dbFieldName = "user_id")
    private Integer userId = 1;

    @MysqlFieldAnnotation(dbFieldName = "username")
    private String username = "xiongsenlin";

    @MysqlFieldAnnotation(dbFieldName = "nickname")
    private String nickname = "xiongsenlin";

    @MysqlFieldAnnotation(dbFieldName = "password")
    private String password = "123456";

    @MysqlFieldAnnotation(dbFieldName = "is_superuser")
    private Integer isSuperuser = 1;

    @MysqlFieldAnnotation(dbFieldName = "is_active", nullable = false)
    private Integer isActive = 1;

    @MysqlFieldAnnotation(dbFieldName = "email")
    private String email = "xiongsenlin2006@126.com";

    @MysqlFieldAnnotation(dbFieldName = "gmt_create")
    private Date gmtCreate = new Date();

    @MysqlFieldAnnotation(dbFieldName = "gmt_modify")
    private Date gmtModify = new Date();

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getNickname() {
        return nickname;
    }

    public void setNickname(String nickname) {
        this.nickname = nickname;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Integer getIsSuperuser() {
        return isSuperuser;
    }

    public void setIsSuperuser(Integer isSuperuser) {
        this.isSuperuser = isSuperuser;
    }

    public Integer getIsActive() {
        return isActive;
    }

    public void setIsActive(Integer isActive) {
        this.isActive = isActive;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public Date getGmtCreate() {
        return gmtCreate;
    }

    public void setGmtCreate(Date gmtCreate) {
        this.gmtCreate = gmtCreate;
    }

    public Date getGmtModify() {
        return gmtModify;
    }

    public void setGmtModify(Date gmtModify) {
        this.gmtModify = gmtModify;
    }
}