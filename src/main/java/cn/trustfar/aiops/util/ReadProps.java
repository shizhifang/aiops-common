package cn.trustfar.aiops.util;

/**
 * 属性文件读取实用工具类
 * 属性文件的位置名称：在类路径下mainconfig.properties
 * 提供静态方法：
 * 		根据key获取对应的值
 * 		根据key获取对应的值，同时指定默认值，即属性文件中没有该key对应的值时，返回默认值
 * 	author：fanshengli  2017/11/2
 */
import java.io.IOException;
import java.util.Properties;

public class ReadProps {
    public String propertiesFile;
    public Properties props;
    public ReadProps(String fileName){
        this.propertiesFile=fileName;
//        静态代码段，在类路径下加载属性文件mainconfig.properties
        props = new Properties();
        try {
            props.load(Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream(propertiesFile));
//            props.load(Thread.currentThread().getContextClassLoader()
//                    .getResourceAsStream("application-"+props.getProperty("spring.profiles.active","dev")+".properties"));
        } catch (IOException e) {
            System.out.println("属性文件不存在");
        }
    }

    /**
     * 根据key获取对应的值
     * @param key
     * @return String类型的值
     */
    public  String getProps(String key) {
        return props.getProperty(key);
    }
    /**
     * 根据key获取对应的值，同时指定默认值，即属性文件中没有该key对应的值时，返回默认值
     * @param key
     * @param defVal
     * @return	String类型的值
     */
    public String getProps(String key, String defVal) {
        return props.getProperty(key, defVal);
    }
    /**
     * 根据key获取对应的值，同时指定默认值，即属性文件中没有该key对应的值时，返回默认值
     * @param key
     * @param defVal
     * @return  long类型的值
     */
    public long getProps(String key, long defVal) {
        long result = defVal;
        try {
            result = Long.parseLong(props.getProperty(key));
        } catch (Exception e) {
            result = defVal;
        }
        return result;
    }
    /**
     * 根据key获取对应的值，同时指定默认值，即属性文件中没有该key对应的值时，返回默认值
     * @param key
     * @param defVal
     * @return  int类型的值
     */
    public  int getProps(String key, int defVal) {
        return (int) getProps(key, (long) defVal);
    }
    /**
     * 根据key获取对应的值，同时指定默认值，即属性文件中没有该key对应的值时，返回默认值
     * @param key
     * @param defVal
     * @return boolean类型的值
     */
    public boolean getProps(String key, boolean defVal) {
        boolean result = defVal;
        try {
            result = Boolean.parseBoolean(props.getProperty(key));
        } catch (Exception e) {
            result = defVal;
        }
        return result;
    }
}
