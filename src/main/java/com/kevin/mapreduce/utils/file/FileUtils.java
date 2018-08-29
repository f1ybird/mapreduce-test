package com.kevin.mapreduce.utils.file;

import com.kevin.mapreduce.utils.date.DateUtils;
import com.kevin.mapreduce.utils.math.RandomUtils;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.*;
import org.slf4j.Logger;

/**
 * @desc:文件工具类
 * @Author:chenssy
 * @date:2014年8月7日
 */
public class FileUtils {
    private static final String FOLDER_SEPARATOR = "/";
    private static final char EXTENSION_SEPARATOR = '.';
    private static final int SIZE = 1024 * 8;
    private static Logger logger = LoggerFactory.getLogger(FileUtils.class);

    /**
     * @param filePath 指定的文件路径
     * @param isNew    true：新建、false：不新建
     * @return 存在返回TRUE，不存在返回FALSE
     * @desc:判断指定路径是否存在，如果不存在，根据参数决定是否新建
     * @autor:chenssy
     * @date:2014年8月7日
     */
    public static boolean isExist(String filePath, boolean isNew) {
        File file = new File(filePath);
        if (!file.exists() && isNew) {
            return file.mkdirs();    //新建文件路径
        }
        return false;
    }

    /**
     * 获取文件名，构建结构为 prefix + yyyyMMddHH24mmss + 10位随机数 + suffix + .type
     *
     * @param type   文件类型
     * @param prefix 前缀
     * @param suffix 后缀
     * @return
     * @autor:chenssy
     * @date:2014年8月11日
     */
    public static String getFileName(String type, String prefix, String suffix) {
        String date = DateUtils.getCurrentTime("yyyyMMddHH24mmss");   //当前时间
        String random = RandomUtils.generateNumberString(10);   //10位随机数

        //返回文件名
        return prefix + date + random + suffix + "." + type;
    }

    /**
     * 获取文件名，文件名构成:当前时间 + 10位随机数 + .type
     *
     * @param type 文件类型
     * @return
     * @autor:chenssy
     * @date:2014年8月11日
     */
    public static String getFileName(String type) {
        return getFileName(type, "", "");
    }

    /**
     * 获取文件名，文件构成：当前时间 + 10位随机数
     *
     * @return
     * @autor:chenssy
     * @date:2014年8月11日
     */
    public static String getFileName() {
        String date = DateUtils.getCurrentTime("yyyyMMddHH24mmss");   //当前时间
        String random = RandomUtils.generateNumberString(10);   //10位随机数

        //返回文件名
        return date + random;
    }

    /**
     * 获取指定文件的大小
     *
     * @param file
     * @return
     * @throws Exception
     * @author:chenssy
     * @date : 2016年4月30日 下午9:10:12
     */
    @SuppressWarnings("resource")
    public static long getFileSize(File file) throws Exception {
        long size = 0;
        if (file.exists()) {
            FileInputStream fis = null;
            fis = new FileInputStream(file);
            size = fis.available();
        } else {
            file.createNewFile();
        }
        return size;
    }

    /**
     * 删除所有文件，包括文件夹
     *
     * @param dirpath
     * @author : chenssy
     * @date : 2016年5月23日 下午12:41:08
     */
    public void deleteAll(String dirpath) {
        File path = new File(dirpath);
        try {
            if (!path.exists())
                return;// 目录不存在退出
            if (path.isFile()) // 如果是文件删除
            {
                path.delete();
                return;
            }
            File[] files = path.listFiles();// 如果目录中有文件递归删除文件
            for (int i = 0; i < files.length; i++) {
                deleteAll(files[i].getAbsolutePath());
            }
            path.delete();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 复制文件或者文件夹
     *
     * @param inputFile   源文件
     * @param outputFile  目的文件
     * @param isOverWrite 是否覆盖文件
     * @throws IOException
     * @author : chenssy
     * @date : 2016年5月23日 下午12:41:59
     */
    public static void copy(File inputFile, File outputFile, boolean isOverWrite)
            throws IOException {
        if (!inputFile.exists()) {
            throw new RuntimeException(inputFile.getPath() + "源目录不存在!");
        }
        copyPri(inputFile, outputFile, isOverWrite);
    }

    /**
     * 复制文件或者文件夹
     *
     * @param inputFile   源文件
     * @param outputFile  目的文件
     * @param isOverWrite 是否覆盖文件
     * @throws IOException
     * @author : chenssy
     * @date : 2016年5月23日 下午12:43:24
     */
    private static void copyPri(File inputFile, File outputFile, boolean isOverWrite) throws IOException {
        if (inputFile.isFile()) {        //文件
            copySimpleFile(inputFile, outputFile, isOverWrite);
        } else {
            if (!outputFile.exists()) {        //文件夹
                outputFile.mkdirs();
            }
            // 循环子文件夹
            for (File child : inputFile.listFiles()) {
                copy(child, new File(outputFile.getPath() + "/" + child.getName()), isOverWrite);
            }
        }
    }

    /**
     * 复制单个文件
     *
     * @param inputFile   源文件
     * @param outputFile  目的文件
     * @param isOverWrite 是否覆盖
     * @throws IOException
     * @author : chenssy
     * @date : 2016年5月23日 下午12:44:07
     */
    private static void copySimpleFile(File inputFile, File outputFile,
                                       boolean isOverWrite) throws IOException {
        if (outputFile.exists()) {
            if (isOverWrite) {        //可以覆盖
                if (!outputFile.delete()) {
                    throw new RuntimeException(outputFile.getPath() + "无法覆盖！");
                }
            } else {
                // 不允许覆盖
                return;
            }
        }
        InputStream in = new FileInputStream(inputFile);
        OutputStream out = new FileOutputStream(outputFile);
        byte[] buffer = new byte[1024];
        int read = 0;
        while ((read = in.read(buffer)) != -1) {
            out.write(buffer, 0, read);
        }
        in.close();
        out.close();
    }

    /**
     * 获取文件的MD5
     *
     * @param file 文件
     * @return
     * @author : chenssy
     * @date : 2016年5月23日 下午12:50:38
     */
    public static String getFileMD5(File file) {
        if (!file.exists() || !file.isFile()) {
            return null;
        }
        MessageDigest digest = null;
        FileInputStream in = null;
        byte buffer[] = new byte[1024];
        int len;
        try {
            digest = MessageDigest.getInstance("MD5");
            in = new FileInputStream(file);
            while ((len = in.read(buffer, 0, 1024)) != -1) {
                digest.update(buffer, 0, len);
            }
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        BigInteger bigInt = new BigInteger(1, digest.digest());
        return bigInt.toString(16);
    }

    /**
     * 获取文件的后缀
     *
     * @param file 文件
     * @return
     * @author : chenssy
     * @date : 2016年5月23日 下午12:51:59
     */
    public static String getFileSuffix(String file) {
        if (file == null) {
            return null;
        }
        int extIndex = file.lastIndexOf(EXTENSION_SEPARATOR);
        if (extIndex == -1) {
            return null;
        }
        int folderIndex = file.lastIndexOf(FOLDER_SEPARATOR);
        if (folderIndex > extIndex) {
            return null;
        }
        return file.substring(extIndex + 1);
    }

    /**
     * 文件重命名
     *
     * @param oldPath 老文件
     * @param newPath 新文件
     * @author : chenssy
     * @date : 2016年5月23日 下午12:56:05
     */
    public boolean renameDir(String oldPath, String newPath) {
        File oldFile = new File(oldPath);// 文件或目录   
        File newFile = new File(newPath);// 文件或目录   
        return oldFile.renameTo(newFile);// 重命名
    }

    /**
     * 读取文件的内容
     * 读取指定文件的内容
     *
     * @param path 为要读取文件的绝对路径
     * @return 以行读取文件后的内容。
     * @since 1.0
     */
    public static final String getFileContent(String path) throws IOException {
        String filecontent = "";
        try {
            File f = new File(path);
            if (f.exists()) {
                FileReader fr = new FileReader(path);
                BufferedReader br = new BufferedReader(fr); //建立BufferedReader对象，并实例化为br
                String line = br.readLine(); //从文件读取一行字符串
                //判断读取到的字符串是否不为空
                while (line != null) {
                    filecontent += line + "\n";
                    line = br.readLine(); //从文件中继续读取一行数据
                }
                br.close(); //关闭BufferedReader对象
                fr.close(); //关闭文件
            }
        } catch (IOException e) {
            throw e;
        }
        return filecontent;
    }

    /**
     * 以字节流的方式读取到字符串。
     *
     * @param is 输入流
     * @return 字符串
     */
    public static String readBytesToString(InputStream is) {
        return new String(readBytes(is));
    }

    /**
     * 以字节流的方式读取到字符串。
     *
     * @param is          输入流
     * @param charsetName 字符集
     * @return 字符串
     */
    public static String readBytesToString(InputStream is, String charsetName) {
        try {
            return new String(readBytes(is), charsetName);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 以字节流的方式从文件中读取字符串
     *
     * @param file        文件
     * @param charsetName 字符集
     * @return 字符串
     */
    public static String readBytesToString(File file, String charsetName) {
        try {
            return new String(readBytes(file), charsetName);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;

    }

    /**
     * 以字节流的方式从文件中读取字符串。
     *
     * @param file 文件
     * @return 字符串
     */
    public static String readBytesToString(File file) {
        return new String(readBytes(file));
    }

    // ---------------------readBytesToString 完成。分割线----------------------

    /**
     * 以字符流的方式读取到字符串。
     *
     * @param file    文件
     * @param charset 字符集
     * @return 字符串
     */
    public static String readCharsToString(File file) {
        try {
            return readCharsToString(new FileInputStream(file), null);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 以字节流的方式读取到字符串。
     *
     * @param file    文件
     * @param charset 字符集
     * @return 字符串
     */
    public static String readCharsToString(File file, String charset) {
        try {
            return readCharsToString(new FileInputStream(file), charset);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 以字符流的方式读取到字符串。默认编码
     *
     * @param is 输入流
     * @return 字符串
     */
    public static String readCharsToString(InputStream is) {
        return new String(readChars(is, null));
    }

    /**
     * 以字符流的方式读取到字符串。
     *
     * @param is          输入流
     * @param charsetName 编码
     * @return 字符串
     */
    public static String readCharsToString(InputStream is, String charsetName) {
        return new String(readChars(is, charsetName));
    }

    // ---------------readCharsToString 完成。分割线-----------------------

    /**
     * 以字节流的方式读取到字符串。
     *
     * @param file 文件
     * @return 字节数组
     */
    public static byte[] readBytes(File file) {
        try {
            return readBytes(new FileInputStream(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 以字节流的方式读取到字符串。
     *
     * @param is 输入流
     * @return 字节数组
     */
    public static byte[] readBytes(InputStream is) {
        byte[] bytes = null;
        try {
            BufferedInputStream bis = new BufferedInputStream(is);

            byte[] cbuf = new byte[SIZE];
            int len;
            ByteArrayOutputStream outWriter = new ByteArrayOutputStream();
            while ((len = bis.read(cbuf)) != -1) {
                outWriter.write(cbuf, 0, len);
            }
            outWriter.flush();

            bis.close();
            is.close();

            bytes = outWriter.toByteArray();
            outWriter.close();

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bytes;
    }

    /**
     * 以字符流的方式读取到字符串。
     *
     * @param file        文件
     * @param charsetName 编码
     * @return 字符数组
     */
    public static char[] readChars(File file, String charsetName) {
        try {
            return readChars(new FileInputStream(file), charsetName);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 以字符流的方式读取到字符串。
     *
     * @param is          输入流
     * @param charsetName 编码
     * @return 字符数组
     */
    public static char[] readChars(InputStream is, String charsetName) {
        char[] chars = null;
        try {
            InputStreamReader isr = null;
            if (charsetName == null) {
                isr = new InputStreamReader(is);
            } else {
                isr = new InputStreamReader(is, charsetName);
            }
            BufferedReader br = new BufferedReader(isr);
            char[] cbuf = new char[SIZE];
            int len;
            CharArrayWriter outWriter = new CharArrayWriter();
            while ((len = br.read(cbuf)) != -1) {
                outWriter.write(cbuf, 0, len);
            }
            outWriter.flush();

            br.close();
            isr.close();
            is.close();

            chars = outWriter.toCharArray();
            outWriter.close();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return chars;
    }

    // -----------------------readxxx 完成。分割线-----------------------
    // -----------------------read 部分完成。接下来是write的部分------------

    /**
     * 通过字节输出流输出bytes
     *
     * @param os   输出流
     * @param text 字节数组
     */
    public static void writeBytes(OutputStream os, byte[] text) {
        writeBytes(os, text, 0, text.length);
    }

    /**
     * 通过字节输出流输出bytes
     *
     * @param os     输出流
     * @param text   字节数组
     * @param off    数组起始下标
     * @param lenght 长度
     */
    public static void writeBytes(OutputStream os, byte[] text, int off, int lenght) {
        try {
            BufferedOutputStream bos = new BufferedOutputStream(os);
            bos.write(text, off, lenght);

            bos.flush();
            bos.close();
            os.close();

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // ----------------------writeByte 完成。分割------------------------

    /**
     * 通过字符输出流输出chars
     *
     * @param os          输出流
     * @param text        字节数组
     * @param charsetName 编码方式
     */
    public static void writeChars(OutputStream os, char[] text, String charsetName) {
        writeChars(os, text, 0, text.length, charsetName);
    }

    /**
     * 通过字符输出流输出chars
     *
     * @param os          输出流
     * @param text        字节数组
     * @param off         数组起始下标
     * @param lenght      长度
     * @param charsetName 编码方式
     */
    public static void writeChars(OutputStream os, char[] text, int off, int lenght, String charsetName) {
        try {
            OutputStreamWriter osw = null;

            if (charsetName == null) {
                osw = new OutputStreamWriter(os);
            } else {
                osw = new OutputStreamWriter(os, charsetName);
            }
            BufferedWriter bw = new BufferedWriter(osw);
            bw.write(text, off, lenght);

            bw.flush();
            bw.close();
            osw.close();
            os.close();

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // ----------------------writeChars 完成。分割------------------------

    /**
     * 将字符串以默认编码写入文件
     *
     * @param file 文件
     * @param text 字符串
     */
    public static void writeString(File file, boolean append, String text) {
        writeString(file, append, text, 0, text.length(), null);
    }

    /**
     * 将字符串写入文件
     *
     * @param file        文件
     * @param append      是否追加
     * @param text        字符串
     * @param off         起始下标
     * @param lenght      长度
     * @param charsetName 编码名称
     */
    public static void writeString(File file, boolean append, String text, int off, int lenght, String charsetName) {
        try {
            writeString(new FileOutputStream(file, append), text, off, lenght, charsetName);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 将字符串以默认编码写入文件
     *
     * @param file 文件
     * @param text 字符串
     */
    public static void writeString(File file, String text) {
        writeString(file, false, text, 0, text.length(), null);
    }

    /**
     * 将字符串写入文件（默认覆盖）
     *
     * @param file        文件
     * @param append      是否追加
     * @param text        字符串
     * @param charsetName 编码名称
     */
    public static void writeString(File file, boolean append, String text, String charsetName) {
        writeString(file, append, text, 0, text.length(), charsetName);
    }

    /**
     * 将字符串写入文件（默认覆盖）
     *
     * @param file        文件
     * @param text        字符串
     * @param charsetName 编码名称
     */
    public static void writeString(File file, String text, String charsetName) {
        writeString(file, false, text, 0, text.length(), charsetName);
    }

    /**
     * 字符输出流输出字符串
     *
     * @param os          输出流
     * @param text        字符串
     * @param charsetName 编码
     */
    public static void writeString(OutputStream os, String text, String charsetName) {
        writeString(os, text, 0, text.length(), charsetName);
    }

    /**
     * 字符输出流输出字符串
     *
     * @param os          输出流
     * @param text        字符串
     * @param off         起始下标
     * @param lenght      长度
     * @param charsetName 编码
     */
    public static void writeString(OutputStream os, String text, int off, int lenght, String charsetName) {
        try {
            OutputStreamWriter osw = null;

            if (charsetName == null) {
                osw = new OutputStreamWriter(os);
            } else {
                osw = new OutputStreamWriter(os, charsetName);
            }
            BufferedWriter bw = new BufferedWriter(osw);
            bw.write(text, off, lenght);

            bw.flush();
            bw.close();
            osw.close();
            os.close();

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String ReadFile(String fileName) {
        String encoding = "utf-8";
        File file = new File(fileName);
        Long filelength = file.length();
        byte[] filecontent = new byte[filelength.intValue()];
        try {
            FileInputStream in = new FileInputStream(file);
            in.read(filecontent);
            in.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            return new String(filecontent, encoding);
        } catch (UnsupportedEncodingException e) {
            System.err.println("The OS does not support " + encoding);
            e.printStackTrace();
            return null;
        }
    }
    public synchronized static void writeFile(String content, String fileName, String encoding,boolean append) {
        try {

            File file = new File(fileName);
            if (!file.exists())
                file.createNewFile();
            FileOutputStream out = new FileOutputStream(file, append);
            StringBuffer sb = new StringBuffer();
            sb.append(content);
            out.write(sb.toString().getBytes(encoding));

            out.close();
        } catch (IOException ex) {
            logger.error(ex.getMessage());

        }
    }

    public static void writeFileByLines(List<String> lsts, String fileName, String encoding, Boolean append) throws IOException {
        File file = new File(fileName);
        if (!file.exists())
            file.createNewFile();
        FileOutputStream out = new FileOutputStream(file, append);

        StringBuffer sb = new StringBuffer();
        for(String tmp:lsts)
        {
            sb.append(tmp + "\n");
        }

        out.write(sb.toString().getBytes(encoding));
        out.close();
    }

    public static List<String> readFileByLines(String fileName,String encoding)
    {
        List<String> lsts = new ArrayList<String>();
        try
        {
            File file = new File(fileName);

            String s = "";
            InputStreamReader isr = new InputStreamReader(new FileInputStream(file), encoding);

            BufferedReader br = new BufferedReader(isr);
            while ((s = br.readLine()) != null) {
                lsts.add(s);
            }

            isr.close();
        }
        catch(Exception ex)
        {
            ex.printStackTrace();
        }

        return lsts;
    }

    public String readCurrent(String fileName) {
        InputStream in = null;
        // 绗竴绉嶆柟娉曪紝鍙栧緱src涓嬬殑灞炴�鏂囦欢锛屾垚鍔�
        String content = "";

        try {
            in = this.getClass().getResourceAsStream(fileName);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            // System.out.println(this.getClass().getResource("/token.txt").getFile());
            // BufferedReader br=new BufferedReader(new InputStreamReader(is));
            String s = "";
            while ((s = br.readLine()) != null)
                content += s + "\n";
            in.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return content;

    }

    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return (o2.getValue()).compareTo(o1.getValue());
            }
        });

        Map<K, V> result = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    public static boolean createDir(String path) {
        File file = new File(path);
        if (!file.exists() && !file.isDirectory()) {
            return true;
        } else {
            return false;
        }

    }

    public static boolean create(String path) {
        File file = new File(path);
        return file.mkdir();
    }

    public static final String[] CHARSET_NAMES = new String[] { "ISO8859-1", "GBK", "UTF-8" };
}
