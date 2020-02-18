package cn.trustfar.aiops.constant;

public class CommonConstants {
    /**
     * dataType
     * 性能、告警、交易数据分类
     */
    public static final int PERFORMANCE=1;
    public static final int AlERT=2;
    public static final int DEAL=3;

    /**
     *
     *OR值得关系是或
     * RANGE值得关系是范围
     * SINGLE值得关系是等于，就只有一个value
     */
    public static final String OR="OR";
    public static final String RANGE="RANGE";
    public static final String SINGLE="SINGLE";


    /**
     * timeType
     * 时间间隔类型
     * minute按分钟间隔
     * hour按小时间隔
     */
    public static final int MINUTE=1;
    public static final int HOUR=2;

    /**
     * dataProcessType
     * 对数据处理类型
     * avg 数据取平均值
     * sum 数据累加
     *
     */
    public static final int AVG=1;
    public static final int SUM=2;
}
