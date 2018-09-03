package com.kevin.mapreduce.utils;

import com.kevin.mapreduce.constants.Constant;
import com.kevin.mapreduce.utils.file.FileUtils;
import org.apache.commons.lang.StringUtils;
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


    public static void main(String[] args) throws Exception{
        String path = TabSplitUtil.class.getClassLoader().getResource("file/in_friend2.txt").getPath();
        System.out.println(path);
//        List<String> list = TabSplitUtil.tabSplit(path);
//        list.stream().forEach(s->{
//            System.out.println(s);
//        });
        TabSplitUtil.replaceByTab(path);

    }

    /**
     * 将文件中 的每行内容间的分隔符由空格改成"\t"
     * @param fileName 文件名 如：C:\file.txt
     * @return
     */
    public static void replaceByTab(String fileName){
        try{
            List<String> lines = FileUtils.readFileByLines(fileName, "UTF-8");
            String writeFileName = fileName.substring(0,fileName.lastIndexOf(File.separator)+1) + "writeFile.txt";
            lines.stream().forEach(s -> {
                if(StringUtils.isNotBlank(s)){
                    String newLine = s.replaceAll(Constant.SPACE_SPLIT_1, Constant.TAB_SPLIT) + "\n";
                    FileUtils.writeFile(newLine,writeFileName,"UTF-8",true);
                }
            });
            LoggerUtil.info("replace tab successful!");
        }catch (Exception e){
            LoggerUtil.error("replace tab failed!");
        }

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
