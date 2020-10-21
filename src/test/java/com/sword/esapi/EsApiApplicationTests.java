package com.sword.esapi;

import com.alibaba.fastjson.JSON;
import com.sword.esapi.pojo.User;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
import org.elasticsearch.script.mustache.SearchTemplateRequestBuilder;
import org.elasticsearch.script.mustache.SearchTemplateResponse;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class EsApiApplicationTests {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    // 测试索引的创建 PUT
    @Test
    void testCreateIndex() throws IOException {
        // 1.创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("kuang_index");
        // 2.执行客户端请求
        // 相当于 PUT kuang_index
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);

        System.out.println(createIndexResponse);
    }

    // 测试获取索引 GET
    // 判断其是否存在
    @Test
    void testExistIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("kuang_index");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }


    // 测试删除索引 DELETE
    @Test
    void testDeleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("kuang_index");
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }


    // 测试添加文档
    @Test
    void testAddDocument() throws IOException {
        // 创建对象
        User user = new User("狂神说",3);
        // 创建请求
        IndexRequest request = new IndexRequest("kuang_index");

        // 规则 put /kuang_index/_doc/1
        request.id("1");
        request.timeout(TimeValue.timeValueSeconds(100));

        // 将数据放入请求  json
        IndexRequest source = request.source(JSON.toJSONString(user), XContentType.JSON);
        // 客户端发送请求
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

        System.out.println(indexResponse.toString());
        System.out.println(indexResponse.status());

    }
//
//    // 获取文档，判断是否存在  get /index/doc/1
//    @Test
//    void testIsExists() throws IOException {
//        GetRequest getRequest = new GetRequest("kuang_index","1");
//        // 不获取返回的 _source  的上下文了
//        getRequest.fetchSourceContext(new FetchSourceContext(false));
//
//        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
//        System.out.println(exists);
//    }
//
//    // 获得文档的信息
//    @Test
//    void testGetDocument() throws IOException {
//        GetRequest getRequest = new GetRequest("kuang_index","1");
//        GetResponse getResponse = client.get(getRequest,RequestOptions.DEFAULT);
//
//        System.out.println(getResponse.getSourceAsString());
//    }
//
//
//    //测试修改文档
//    @Test
//    void testUpdateDocument() throws IOException {
//        User user = new User("李逍遥", 55);
//        //修改是id为1的
//        UpdateRequest request= new UpdateRequest("kuang_index","1");
//        request.timeout("100s");
//        request.doc(JSON.toJSONString(user),XContentType.JSON);
//        UpdateResponse response = client.update(request,
//                RequestOptions.DEFAULT);
//        System.out.println("测试修改文档-----"+response);
//        System.out.println("测试修改文档-----"+response.status());
//    }
//
//    //测试删除文档
//    @Test
//    void testDeleteDocument() throws IOException {
//        DeleteRequest request= new DeleteRequest("kuang_index","1");
//        request.timeout("100s");
//        DeleteResponse response = client.delete(request,
//                RequestOptions.DEFAULT);
//        System.out.println("测试删除文档------"+response.status());
//    }

    // 批量插入数据
    @Test
    void testBulkRequest() throws IOException{
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("100s");

        ArrayList<User> usersList = new ArrayList<>();
        usersList.add(new User("kaungshen2",3));
        usersList.add(new User("kaungshen3",3));
        usersList.add(new User("kaungshen5",3));
        usersList.add(new User("kaungshen6",3));
        usersList.add(new User("kaungshen7",3));
        usersList.add(new User("kaungshen8",3));
        usersList.add(new User("kaungshen9",3));
        usersList.add(new User("kaungshen25",3));
        usersList.add(new User("kaungshen23",3));

        for (int i = 0; i < usersList.size(); i++) {
            bulkRequest.add(
                    new IndexRequest("kuang_index")
                    .id(""+(i+1))
                    .source(JSON.toJSONString(usersList.get(i)),XContentType.JSON));
        }
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulkResponse.hasFailures());

    }

    // 查询
    @Test
    void testSearch() throws IOException {
//        SearchRequest searchRequest = new SearchRequest("kuang_index");

//        // 构建搜索条件
//        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//
//        // 查询条件，可以使用QueryBuilders 工具来实现
//        // QueryBuilders.termQuery 精确
//        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "kaungshen2");
//
//        // QueryBuilders.matchAllQuery 匹配所有
//        // MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
//        searchSourceBuilder.query(termQueryBuilder);
//        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));



          // Search_After 分页
//        用SearchAfterBuilder API:  不行

//        // 这个client是TransportClient，在8.0版本后要被弃用，es不推荐使用
//        // 本来SearchRequestBuilder是要通过TransportClient获得的
//        TransportClient transportClient = new PreBuiltTransportClient(settings)
//                .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300))
//                .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9301));
//        SearchRequestBuilder searchRequestBuilder = client.prepareSearch().setTypes
//        SearchAfterBuilder searchAfterBuilder = new SearchAfterBuilder();
//        searchAfterBuilder.setSortValues(new Object[]{5,"kuangshen7"});
//        searchRequest.source(searchSourceBuilder);
//        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        // 用search template：

        SearchTemplateRequest request = new SearchTemplateRequest();
        request.setRequest(new SearchRequest("listen"));

        request.setScriptType(ScriptType.INLINE);
        request.setScript(
                        "{\n" +
                        "  \"size\": {{size}},\n" +
                        "  \"query\": {\n" +
                        "    \"match\": {\n" +
                        "      \"{{field1}}\": \"{{value1}}\"\n" +
                        "    }\n" +
                        "  },\n" +
                        "  \"search_after\": [{{value}}],\n" +
                        "    \"sort\": [\n" +
                        "        {\"{{field2}}\": \"{{value2}}\"}\n" +
                        "    ]\n" +
                        "}"
        );

        Map<String, Object> scriptParams = new HashMap<>();
        scriptParams.put("size", 2);
        scriptParams.put("field1", "name");
        scriptParams.put("value1", "xxx");
        scriptParams.put("value", 2);
        scriptParams.put("field2", "age");
        scriptParams.put("value2", "asc");
        request.setScriptParams(scriptParams);
        // 用于render
//        request.setSimulate(true);
        SearchTemplateResponse response = client.searchTemplate(request, RequestOptions.DEFAULT);
        SearchResponse searchResponse = response.getResponse();

        System.out.println("============================");
        System.out.println(JSON.toJSONString(searchResponse.getHits()));
        System.out.println("============================");
    }

    /**
     * ============================
     * {"fragment":true,"hits":[
     *
     * {"fields":{},"fragment":false,"highlightFields":{},"id":"4","matchedQueries":[],"primaryTerm":0,"rawSortValues":[],"score":null,"seqNo":-2,"sortValues":[3],
     *
     *      "sourceAsMap":{"name":"xxx","age":3},"sourceAsString":"{\"name\":\"xxx\",\"age\":3}",
     *      "sourceRef":{"fragment":true},"type":"try","version":-1},
     *
     * {"fields":{},"fragment":false,"highlightFields":{},"id":"3","matchedQueries":[],"primaryTerm":0,"rawSortValues":[],"score":null,"seqNo":-2,"sortValues":[4],
     *
     *      "sourceAsMap":{"name":"xxx","age":4},"sourceAsString":"{\"name\":\"xxx\",\"age\":4}","
     *      sourceRef":{"fragment":true},"type":"try","version":-1}],
     *
     * "maxScore":null,"totalHits":{"relation":"EQUAL_TO","value":4}}
     * ============================
     */
}
