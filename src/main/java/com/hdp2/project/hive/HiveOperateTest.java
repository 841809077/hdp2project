package com.hdp2.project.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author liuyzh
 * @description: 基于Kerberos环境，使用Java远程执行Hive操作
 * date 2019/8/7 16:22
 */
public class HiveOperateTest {

    private static String url = "jdbc:hive2://node72.xdata:10000/default;principal=hive/node72.xdata@XDATA.COM";
    private static String url2 = "jdbc:hive2://node71.xdata:2181,node72.xdata:2181,node73.xdata:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;principal=hive/node72.xdata@XDATA.COM";
    private static Connection conn = null;
    private static PreparedStatement ps = null;
    private static ResultSet rs = null;

    /**
     * @description: 通过jdbc连接hive2
     */
    @Test
    @Before
    public void getConnection() {
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        System.setProperty("krb5_ini", System.getProperty("user.dir") + "\\krb5\\krb5.ini");
        System.setProperty("hive_keytab", System.getProperty("user.dir") + "\\krb5\\liuyzh.service.keytab");
        System.setProperty("java.security.krb5.conf", System.getProperty("krb5_ini"));
        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab("liuyzh/node71.xdata@XDATA.COM", System.getProperty("hive_keytab"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            // 使用hive用户登陆
            conn = DriverManager.getConnection(url2, "", "");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * @description: 创建数据库
     */
    @Test
    public void cresteDatabase(){
        try {
            conn.prepareStatement("CREATE database test").execute();
            System.out.println("数据库创建成功");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * @description: 查询数据
     */
    @Test
    public void getAll() {
        String sql = "select * from mytable";
        System.out.println(sql);
        try {
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            // 获取所有列
            int columns = rs.getMetaData().getColumnCount();
            //处理数据
            while (rs.next()) {
                for (int i = 1; i <= columns; i++) {
                    System.out.print(rs.getString(i) + "\t");
                }
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * @description: 执行DDL语句
     */
    @Test
    public void execute() {
        String sql = "INSERT overwrite directory '/user/hue/learn_oozie/mazy_hive_1/output'\n" +
                "row format delimited fields terminated by \"\\t\"\n" +
                "SELECT sid,sname FROM mytable LIMIT 10";
        System.out.println(sql);
        try {
            ps = conn.prepareStatement(sql);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * @description: 创建表格
     */
    @Test
    public void createTables() {
        String createSql = "create table if not exists default.mytable(id String, name String) row format delimited fields terminated by \",\" stored as textfile";
        try {
            conn.prepareStatement(createSql).execute();
            System.out.println("创建表成功");
        } catch (SQLException e) {
            e.printStackTrace();
            System.err.println("创建表失败");
        }
    }

    /**
     * @description: 进入数据库，展示所有表
     */
    @Test
    public void showTables() {
        try {
            // 进入default数据库
            ps = conn.prepareStatement("use default");
            ps.execute();
            // 展示所有表
            rs = ps.executeQuery("show tables");
            // 处理结果集
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * @description: 加载数据
     */
    @Test
    public void loadTable() {
        String loadSql = "load data inpath \"/user/liuyzh/mytable.txt\" into table mytable";
        try {
            conn.prepareStatement(loadSql).execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * @description: 删除数据表
     */
    @Test
    public void deleteTable(){
        try {
            conn.prepareStatement("DROP table default.mytable").execute();
            System.out.println("数据表被删除");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * @description: 删除数据库
     */
    @Test
    public void dropDatabase(){
        String sql = "drop database test";
        try {
            conn.prepareStatement(sql).execute();
            System.out.println("删除数据库成功");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    /**
     * @description: 关闭连接
     */
    @Test
    @After
    public void closeConnect() {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
