package cn.trustfar.aiops.util;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ElasticSearch 连接池工具类
 *
 */
public class ElasticSearchPoolUtil {
    private final static Logger LOGGER = LoggerFactory.getLogger( ElasticSearchPoolUtil.class );
    /**
     * 默认连接池的大小为8
     */
    private static final int MAX_TOTAL = 8;
    /**
     * 默认的scheme是http
     */
    private static final String SCHEME = "http";

    /**
     * 连接池的缓存
     */
    private final Map<String, GenericObjectPool<RestHighLevelClient>> elasticPool = new ConcurrentHashMap<>( 16 );

    /**
     * 获取elastic 链接
     *
     * @param ip
     * @return
     */
    public RestHighLevelClient getElasticClient(String ip) {
        RestHighLevelClient client = null;
        synchronized (this.elasticPool) {
            GenericObjectPool<RestHighLevelClient> pool = elasticPool.get( ip.trim() );
            if (null == pool) {
                // 对象池配置类，不写也可以，采用默认配置
                GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
                // 采用默认配置maxTotal是8，池中有8个client
                poolConfig.setMaxTotal( MAX_TOTAL );
                // 要池化的对象的工厂类，这个是我们要实现的类
                EsClientPoolFactory esClientPoolFactory = new EsClientPoolFactory( ip.trim(), SCHEME );
                // 利用对象工厂类和配置类生成对象池
                pool = new GenericObjectPool<>( esClientPoolFactory,
                        poolConfig );

                this.elasticPool.put( ip.trim(), pool );
            }
            // 从池中取一个对象
            try {
                client = pool.borrowObject();
            } catch (Exception e) {
                LOGGER.error( "elastic cache tool error:{}", e );
            }
        }
        return client;
    }

    /**
     * 释放Elastic连接
     *
     * @param ip
     * @param restClient
     */
    public void releaseClient(String ip, RestHighLevelClient restClient) {
        GenericObjectPool<RestHighLevelClient> genericObjectPool = this.elasticPool.get( ip.trim() );
        if (null != genericObjectPool) {
            genericObjectPool.returnObject( restClient );
        }
    }




}