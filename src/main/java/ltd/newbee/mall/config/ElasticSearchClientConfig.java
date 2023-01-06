package ltd.newbee.mall.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticSearchClientConfig {
    @Bean
    public RestHighLevelClient restHighLevelClient(){
        RestHighLevelClient client=new RestHighLevelClient(
                RestClient.builder(new HttpHost("192.168.152.130",9200,"http"))
        );
        return client;
    }
}
