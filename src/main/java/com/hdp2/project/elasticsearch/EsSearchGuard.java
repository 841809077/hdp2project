package com.hdp2.project.elasticsearch;

import com.floragunn.searchguard.ssl.SearchGuardSSLPlugin;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.URL;
import java.net.URLDecoder;

/**
 * @author Administrator
 * @description
 * @date 2020/4/27
 */
public class EsSearchGuard {

    public static void main(String[] args) throws Exception {

        ClassLoader classLoader = ElasticSearchClient.class.getClassLoader();
//        URL resource = classLoader.getResource("node-1-keystore.jks");
        URL resource = classLoader.getResource("liuyzh-keystore.jks");
        URL truresource = classLoader.getResource("truststore.jks");
        String keypath = URLDecoder.decode(resource.getPath(), "UTF-8");
        String trupath = URLDecoder.decode(truresource.getPath(),"UTF-8");
        //windows中路径会多个/ 如/E windows下需要打开注释
        if (keypath.startsWith("/")) {

            keypath=keypath.substring(1, keypath.length());
        }
        if (trupath.startsWith("/")) {
            trupath = trupath.substring(1, trupath.length());
        }

        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .put("searchguard.ssl.transport.enabled", true)
                .put("searchguard.ssl.transport.keystore_filepath", keypath)
                .put("searchguard.ssl.transport.truststore_filepath", trupath)
                //这里的password去生成的证书文件 readme中找
                .put("searchguard.ssl.transport.keystore_password", "kspass")
                .put("searchguard.ssl.transport.truststore_password", "tspass")
                .put("searchguard.ssl.transport.enforce_hostname_verification", false)
                    .put("client.transport.ignore_cluster_name", true)
                    .put("searchguard.ssl.transport.resolve_hostname", true)
                .build();

        TransportClient client = new PreBuiltTransportClient(settings, SearchGuardSSLPlugin.class)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.66.193"), 9301));

        client.admin().cluster().nodesInfo(new NodesInfoRequest()).actionGet();

        //搜索数据
        GetResponse response = client.prepareGet("face_card", "doc", "1").execute().actionGet();
        //输出结果
        System.out.println(response.getSourceAsString());
        //关闭client
        client.close();
    }


}
