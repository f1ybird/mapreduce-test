package com.kevin.mapreduce.mr.friendRecom;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 
 * @author liu
 *  将key根据用户名和次数排序
 */
public class FofSort extends WritableComparator{

    public FofSort() {
        super(User.class,true);
    }

    public int compare(WritableComparable a,WritableComparable b){
        User u1 = (User) a;
        User u2 = (User) b;

        int result = u1.getName().compareTo(u2.getName());  //比较用户名
        if(result == 0){
            return -Integer.compare(u1.getFriendsCount(), u2.getFriendsCount());  //比较次数
        }
        return result;

    }
}