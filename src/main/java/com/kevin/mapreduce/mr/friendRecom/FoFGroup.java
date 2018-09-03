package com.kevin.mapreduce.mr.friendRecom;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * describe  : 将同一个用户作为一个Group，同时被reduce处理
 * creat_user: kevin
 * creat_time: 2018/9/3 22:33
 * email     : kevin_love_it@163.com
 **/
public class FoFGroup extends WritableComparator {

    public FoFGroup() {
        super(User.class, true);
    }

    public int compare(WritableComparable a, WritableComparable b) {
        User u1 = (User) a;
        User u2 = (User) b;

        return u1.getName().compareTo(u2.getName());  //比较是否为同一个用户
    }
}
