package com.kevin.mapreduce.utils;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * describe  : 工具类
 * creat_user: zhangkai
 * creat_time: 2018/8/26 22:19
 * email     : kevin_love_it@163.com
 **/
public class Helper {

    /**
     * 私有空构造函数，无需创建实例
     */
    private Helper() {
        // empty
    }

    /**
     * 将泛型类型为Object的List转化为指定类型的List
     * 
     * @param source
     *            用于转化的List
     * @param targetClass
     *            要转化成的类型
     * @return 指定类型的List
     */
    public static <T> List<T> cast(List<Object> source, Class<T> targetClass) {
        List<T> result = new ArrayList<T>();
        for (Object obj : source) {
            result.add(targetClass.cast(obj));
        }
        return result;
    }
    
    
    /**
     * 构造泛型类型为指定类型的List，后面的参数必须可转为指定类型
     * 
     * @param clazz
     *            List的泛型
     * @param objects
     *            要插入list的数组
     * @return 泛型类型为指定类型的List
     */
    public static <T> List<T> getList(Class<T> clazz, Object... objects) {
        List<T> list = new ArrayList<T>();
        for (Object obj : objects) {
            list.add(clazz.cast(obj));
        }
        return list;
    }

    /**
     * 将所给对象value转换成所给的类型
     * 
     * @param value
     *            用于转换的对象
     * @param clz
     *            要转换的目标类型
     * @return 转换后的对象，如果找不到对应的转换类，则返回null
     */
    private static <T> T cast(Object value, Class<T> clz) {
        String str = value.toString();
        if (String.class.equals(clz)) {
            return clz.cast(str);
        } else if (Long.class.equals(clz)) {
            return clz.cast(Long.valueOf(str));
        } else if (Integer.class.equals(clz)) {
            return clz.cast(Integer.valueOf(str));
        }

        return null;
    }

    /**
     * 判断目录对象是否为null，为空则为false， 否则为true
     * 
     * @param obj
     *            用于判断的对象
     * @return 布尔值，false表示null
     */
    public static boolean notNull(Object obj) {
        if (null == obj) {
            return false;
        }
        return true;
    }
    
    /**
     * 判断目标对象是否既不为null，又不为空，满足则为true, 否则为false
     * 
     * @param str
     *            用于判断的对象
     * @return true表示对象不为null且不为空
     */
    public static boolean notNullNorEmpty(String str) {
        if (notNull(str)) {
            if (!"".equals(str.trim())) {
                return true;
            }
        }
        return false;
    }

    /**
     * 判断目标对象是否既不为null，又不为空，满足则为true, 否则为false
     * 
     * @param list
     *            用于判断的对象
     * @return true表示对象不为null且不为空
     */
    @SuppressWarnings("rawtypes")
    public static boolean notNullNorEmpty(List list) {
        if (notNull(list)) {
            if (!list.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    /**
     * 通过给定的Timestamp对象生成年月的数值
     * 
     * @param time
     *            Timestamp对象
     * @return 年月的整数值
     */
    public static Integer getMon(Timestamp time) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time.getTime());
        int mon = (calendar.get(Calendar.MONTH) + 1);
        return Integer.valueOf(calendar.get(Calendar.YEAR) + "" + (mon > 9 ? mon : ("0" + mon)));
    }
    
    /**
     * 将字符串转为<code>Timestamp</code>对象
     * @param str
     *        要转为<code>Timestamp</code>对象的字符串
     * @return 转换后的<code>Timestamp</code>对象
     */
    public static Timestamp toTimestamp(String str) {
        Timestamp result = null;
        if (str.length() < 18 && str.length() >= 10) {
            result = Timestamp.valueOf(str.substring(0, 10) + " 00:00:00");
        } else if (str.length() >= 18) {
            result = Timestamp.valueOf(str.substring(0, 19));
        }

        return result;
    }

    public static String join(String[] arr, String separate) {
        StringBuffer sb = new StringBuffer();
        for (String str : arr) {
            append(sb, str, separate);
        }
        return sb.toString();
    }


    public static void append(StringBuffer left, String right, String separate) {
        if (Helper.notNull(left) && Helper.notNullNorEmpty(left.toString())) {
            if (Helper.notNullNorEmpty(right)) {
                left.append(separate);
            }
        }
        left.append(right);
    }

}