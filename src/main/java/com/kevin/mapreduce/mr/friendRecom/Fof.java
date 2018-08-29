package com.kevin.mapreduce.mr.friendRecom;

import org.apache.hadoop.io.Text;

/**
 * describe  :简单好友推荐
 *
 * 说明：好友a:b之间与好友b:a之间是一样的，为了在reduce阶段，key相同可以自动合并，
 * 故采用字典排序统一规范好友之间的名字列表
 *
 * creat_user: zhangkai
 * creat_time: 2018/8/29 23:20
 * email     : kevin_love_it@163.com
 **/
public class Fof extends Text {
    public Fof() {
        super();
    }

    public Fof(String a, String b) {
        super(getFof(a, b));
    }

    /**
     * 统一确定a与b的排序格式
     * @param a
     * @param b
     * @return
     */
    public static String getFof(String a, String b) {
        int r = a.compareTo(b);
        if (r < 0) {
            return a + "\t" + b;
        } else {
            return b + "\t" + a;
        }
    }
}