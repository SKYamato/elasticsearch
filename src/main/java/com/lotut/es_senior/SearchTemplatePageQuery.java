package com.lotut.es_senior;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
import org.elasticsearch.script.mustache.SearchTemplateRequestBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author shuai
 * @date 2019/10/12 14:24
 */
public class SearchTemplatePageQuery {
    public static void main(String[] args) throws UnknownHostException {
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        Map<String, Object> scriptParams = new HashMap<String, Object>(3);
        scriptParams.put("from", 0);
        scriptParams.put("size",1);
        scriptParams.put("brand","宝马");
        SearchResponse searchResponse = new SearchTemplateRequestBuilder(client)
                .setScript("page_query_by_brand")
                .setScriptType(ScriptType.FILE)
                .setScriptParams(scriptParams)
                .setRequest(new SearchRequest("car_shop").types("cars"))
                .get()
                .getResponse();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }
        client.close();
    }
}
