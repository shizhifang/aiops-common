package cn.trustfar.aiops.util;



import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * EliasticSearch连接池工厂对象
 */
public class EsClientPoolFactory implements PooledObjectFactory<RestHighLevelClient>{

    private HttpHost[] hosts;


    public EsClientPoolFactory(String hostName, String scheme) {
        String[] split = hostName.split( "," );
        this.hosts = new HttpHost[split.length];
        for (int i = 0; i < split.length; i++) {
            String[] host = split[i].split( ":" );
            HttpHost info = new HttpHost( host[0], Integer.parseInt( host[1] ), scheme );
            this.hosts[i] = info;
        }
    }

    @Override
    public void activateObject(PooledObject<RestHighLevelClient> arg0) throws Exception {
        System.out.println("activateObject");

    }

    /**
     * 销毁对象
     */
    @Override
    public void destroyObject(PooledObject<RestHighLevelClient> pooledObject) throws Exception {
        RestHighLevelClient highLevelClient = pooledObject.getObject();
        highLevelClient.close();
    }

    /**
     * 生产对象
     */
//  @SuppressWarnings({ "resource" })
    @Override
    public PooledObject<RestHighLevelClient> makeObject() throws Exception {
//      Settings settings = Settings.builder().put("cluster.name","elasticsearch").build();
        RestHighLevelClient client = null;
        try {
            /*client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"),9300));*/
          //  HttpHost httpHost=new HttpHost("192.168.1.121", 9200, "http");

            RestClientBuilder restClientBuilder= RestClient.builder(hosts);
            client = new RestHighLevelClient(restClientBuilder);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return new DefaultPooledObject<RestHighLevelClient>(client);
    }

    @Override
    public void passivateObject(PooledObject<RestHighLevelClient> arg0) throws Exception {
        System.out.println("passivateObject");
    }

    @Override
    public boolean validateObject(PooledObject<RestHighLevelClient> arg0) {
        return true;
    }
}