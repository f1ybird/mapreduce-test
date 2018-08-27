package com.kevin.mapreduce.utils;

import com.kevin.mapreduce.constants.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class TabSplitUtil {

    private static Logger log = LoggerFactory.getLogger(TabSplitUtil.class);


    public static void main(String[] args){
        String path = TabSplitUtil.class.getClassLoader().getResource("file/in_file.txt").getPath();
        System.out.println(path);
        List<String> list = TabSplitUtil.tabSplit(path);
        list.stream().forEach(s->{
            System.out.println(s);
        });
    }

    /**
     * 读取文件内容，并使用4个空格分隔后返回一个list
     * @param fileName
     * @return
     */
    public static List<String> tabSplit(String fileName){
        List<String> list = new ArrayList<String>();
        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        try {
            fis = new FileInputStream(new File(fileName));
            isr = new InputStreamReader(fis, "UTF-8");
            br = new BufferedReader(isr);
            String line = "";
            while ((line = br.readLine()) != null) {
                String[] split = line.split(Constant.TAB_SPLIT_4);
                for (String str : split) {
                    list.add(str);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try{
                br.close();
                isr.close();
                fis.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return list;
    }
}
