package com.naruto.chapter01;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Stream;

/**
 * MyBatis 设计之初为面向过程的持久层框架，它支持定制化 SQL (增删改查)、存储过程以及高级映射(一对多,多对多);
 * 使用简单的 XML 或注解来配置和映射原生信息，将接口和POJOs(Plain Old Java Objects,普通的Java对象)映射成数据库中的记录。
 * 也有一些ORM的插件,如 tkmybatis (通用mapper,含有很多通用针对数据的crud的方法)
 * 区别于Hibernate的ORM(面向对象)的持久层框架
 *
 * 数据库 和 服务应用 之间通过Socket连接(TCP)
 * 在没有使用数据库连接池的情况下,每次传输耗时耗资源
 * 1. 创建连接 Connection(TCP 存在三次握手,耗时)
 * 2. 传输数据(crud)
 * 3. 断开连接 Disconnect(TCP四次挥手)
 *
 * JDBC缺点
 *   频繁创建和打开、关闭数据连接，太消耗资源
 *   Sql语句存在硬编码，不利于维护
 *   Sql参数设置硬编码，不利于维护
 *   结果集获取与遍历复杂，存在硬编码，不利于维护，手动将数据转成javabean
 *
 * Hibernate
 * 四个状态
 * 1.瞬时态(临时态, User user = new User(),没有和session建立连接)
 * 2.持久态(session.save(user),和session建立连接,id关联起来)
 * 3.游离态(session.clear,和session断开连接)
 * 4.删除态(session.delete(user))
 *
 * 2.3.1.	hibernate缺点
 *   SQL优化方面
 * Hibernate的查询会将表中的所有字段查询出来，这一点会有性能消耗。
 * (select * 存在封包问题: 一次传输数据量是有限的,数据量过大,那么就需要装成多个包,增加了IO次数)
 * Hibernate也可以自己写SQL来指定需要查询的字段，但这样就破坏了Hibernate开发的简洁性。
 *
 *   JDBC
 * Hibernate是在JDBC上进行了一次封装，相比原生SQL性能要低些。
 *
 *   学习成本
 * Hibernate提供了诸多功能和特性。要全掌握很难。
 *
 *   动态SQL
 * Hibernate不支持动态SQL
 *
 *
 * 重量级  轻量级
 * hibernate相对重量级,在执行一个方法时,需要启动或加载很多相关的对象,方法 导致一个方法的使用十分占用系统性能
 *
 * 侵入式
 * 使用一个框架,需要继承框架的一些库(jar),实现框架的接口
 *
 * mybatis
 * 官网文档：http://www.mybatis.org/mybatis-3/zh/index.html
 * 下载：https://github.com/mybatis/mybatis-3/releases
 *
 *
 * 1、导包 Maven
 * 2、创建核心配置文件 mybatis.xml
 * 3、创建SqlSessionFactory会话工厂
 * 4、创建SqlSession会话
 * 5、配置映射文件信息Mapper.xml----SQL (可以生成直接修改,无需反编译)
 * 6、通过SqlSession实现增删改查 (查询不需要提交事务)
 * 7、提交事务
 * 8、关闭资源
 */
public class MybatisInfo {

    private static final Logger logger = LogManager.getLogger(MybatisInfo.class);

    private static List<String> list = new ArrayList<String>() {
        {
            add("N");
            add("N");
            add("N");
            add("1");
            add("2");
            add("3");
            add("1");
            add("2");
            add("3");
            add("4");
            add("5");
            add("6");
            add("N");
            add("N");
            add("N");
            add("N");
            add("N");
            add("N");
            add("N");
            add("N");
            add("N");
            add("N");
            add("1");
            add("2");
        }
    };


    public static void main(String[] args) {
        ArrayList<Integer> integerArrayList = new ArrayList<>();
        list.forEach(x -> {
            collectOverdueMonthList(integerArrayList, x);
        });
        if (null != integerArrayList && !integerArrayList.isEmpty()){
            long count = (long) integerArrayList.size();
            System.out.println(integerArrayList);

            System.out.println(count);
            Optional<Integer> max = integerArrayList.stream()
                    .max(Integer::compareTo);
            System.out.println(max.get());

            //针对准贷记卡
            long count2 = integerArrayList.stream()
                    .filter(x -> x > 2).count();
            System.out.println(count2);
            Optional<Integer> max2 = integerArrayList.stream()
                    .filter(x -> x > 2)
                    .max(Integer::compareTo);
            System.out.println(max2.get());
        }

    }

    public static void collectOverdueMonthList(List<Integer> integerList, String str){
        if (isNumeric(str)) integerList.add(Integer.parseInt(str));
    }


    public static boolean isNumeric(String str){
        if (StringUtils.isBlank(str)) return false;
        return StringUtils.isNumeric(str);
    }
}
