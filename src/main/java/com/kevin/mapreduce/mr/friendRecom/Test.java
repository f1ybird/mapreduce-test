package com.kevin.mapreduce.mr.friendRecom;

import com.kevin.mapreduce.constants.Constant;
import com.kevin.mapreduce.utils.TabSplitUtil;
import com.kevin.mapreduce.utils.file.FileUtils;

import java.util.List;

public class Test {

    public static void main(String[] args){
        String path = TabSplitUtil.class.getClassLoader().getResource("file/in_friend.txt").getPath();
        List<String> lines = FileUtils.readFileByLines(path, Constant.CHARSET_UTF_8);
        lines.stream().forEach(s -> {
            System.out.println("line:" + s);
            String s1 = s.replaceAll(Constant.SPACE_SPLIT_1, "x");
            System.out.println(s1);
        });
//        String[] friends = StringUtils.split(value.toString(), Constant.SPACE_SPLIT_1);
    }
}
