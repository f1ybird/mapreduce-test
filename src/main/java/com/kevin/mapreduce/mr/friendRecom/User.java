package com.kevin.mapreduce.mr.friendRecom;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author liu
 * User作为key和value，User：用户名+FOF系数
 * 用户名一致，FOF系数从大到小排序。 很容易得到一个用户的好友推介的列表
 */
public class User implements WritableComparable {

    private String name;
    private int friendsCount;

    public User() {}

    public User(String name, int friendsCount) {
        this.name = name;
        this.friendsCount = friendsCount;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getFriendsCount() {
        return friendsCount;
    }

    public void setFriendsCount(int friendsCount) {
        this.friendsCount = friendsCount;
    }

    //反序列化
    public void readFields(DataInput arg0) throws IOException {
        this.name = arg0.readUTF();
        this.friendsCount = arg0.readInt();
    }

    //序列化
    public void write(DataOutput arg0) throws IOException {
        arg0.writeUTF(name);
        arg0.writeInt(friendsCount);
    }

    //判断是否为同一用户
    @Override
    public int compareTo(Object o) {
        User u = (User)o;
        int result = this.name.compareTo(u.name);
        if (result == 0) {
            return Integer.compare(this.friendsCount, u.friendsCount);
        }
        return result;
    }

}

