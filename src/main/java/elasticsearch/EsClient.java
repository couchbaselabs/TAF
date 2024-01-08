package elasticsearch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.BulkRequest.Builder;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import jakarta.json.JsonObject;
import reactor.util.function.Tuple2;

public class EsClient {
	public String serverUrl = "http://localhost:9200";
	public String apiKey = null;
	public String user = null;
	public String pwd = null;
	public RestClient restClient;
	public ElasticsearchClient esClient;

	public EsClient(String serverUrl, String apiKey) {
		super();
		this.serverUrl = serverUrl;
		this.apiKey = apiKey;
	}

	public EsClient(String serverUrl, String user, String pwd) {
		super();
		this.serverUrl = serverUrl;
		this.user = user;
		this.pwd = pwd;
	}

	public EsClient() {
		super();
	}

	public void initializeSDK() {

		// Create the low-level client
		this.restClient = RestClient
				.builder(HttpHost.create(this.serverUrl))
				.setDefaultHeaders(new Header[]{
						new BasicHeader("Authorization", "ApiKey " + this.apiKey)
				})
				.build();

		// Create the transport with a Jackson mapper
		ElasticsearchTransport transport = new RestClientTransport(
				restClient, new JacksonJsonpMapper());

		// And create the API client
		this.esClient = new ElasticsearchClient(transport);

	}

	public Response deleteESIndex(String indexName) {
		//delete an Index with indexName"
		Request deleteIndex = new Request("DELETE", "/" + indexName);
		Response deleteResponse = null;
		try {
			deleteResponse = restClient.performRequest(deleteIndex);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return deleteResponse;
	}

	public Response createESIndex(String indexName, JsonObject indexMapping) {
		//create an Index with indexName"
		Request createIndex = new Request("PUT", "/" + indexName);
		try {
			restClient.performRequest(createIndex);
		} catch (IOException e) {
			e.printStackTrace();
		}

		String endpoint = "/" + indexName + "/_mapping";
		String jsonString = "{\n" +
				"  \"properties\": {\n" +
				"    \"embedding\": {\n" +
				"      \"type\": \"dense_vector\",\n" +
				"      \"similarity\": \"l2_norm\",\n" +
				"      \"dims\": 384\n" +
				"    },\n" +
				"    \"productID\": {\n" +
				"      \"type\": \"text\"\n" +
				"    },\n" +
				"    \"productDescription\": {\n" +
				"      \"type\": \"text\",\n" +
				"      \"fields\": {\n" +
				"        \"keyword\": {\n" +
				"          \"type\": \"keyword\",\n" +
				"          \"ignore_above\": 256\n" +
				"        }\n" +
				"      }\n" +
				"    }\n" +
				"  }\n" +
				"}";


		HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);

		Request request = new Request("PUT", endpoint);
		request.setEntity(entity);

		//using low level client
		Response indexResponse = null;
		try {
			indexResponse = restClient.performRequest(request);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return indexResponse;
	}

	public BulkResponse insertDocs(String indexName, List<Tuple2<String, Object>> docs) {
		BulkResponse esResult = null;
		Builder br = new Builder();
		try {
			for(Tuple2<String, Object> doc: docs) {
				br.operations(op -> op           
						.index(i -> i
								.index(indexName)
								.id(doc.getT1())
								.document(doc.getT2())
								)
						);
			}
			esResult = esClient.bulk(br.build());
		} catch (ElasticsearchException | IOException e) {
			e.printStackTrace();
		}

		return esResult;
	}
	
	public BulkResponse deleteDocs(String indexName, List<String> docs) {
		BulkResponse esResult = null;
		Builder br = new Builder();
		try {
			for(String doc: docs) {
				br.operations(op -> op           
						.delete(i -> i
								.index(indexName)
								.id(doc)
								)
						);
			}
			esResult = esClient.bulk(br.build());
		} catch (ElasticsearchException | IOException e) {
			e.printStackTrace();
		}
		return esResult;
	}

	public String performKNNSearch(String indexName,
			String vectorField, float[] fs, int k) throws Exception {
		String endpoint = "/" + indexName + "/_search";

		Map<String, Object> knnQuery = new HashMap<String, Object>();
		knnQuery.put("field", vectorField);
		knnQuery.put("k", k);
		knnQuery.put("num_candidates", k);

		knnQuery.put("query_vector", fs);

		Map<String, Object> requestPayload = new HashMap<>();
		requestPayload.put("knn", knnQuery);
		requestPayload.put("size", k);
//		Map<String, Object> excludes = new HashMap<String, Object>();
//		excludes.put("exclude", new String[] {"embedding"});
		requestPayload.put("_source", new ArrayList<String>(Arrays.asList("productID", "productDescription")));

		//Serialize the request payload as JSON
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

		ObjectWriter objectWriter = objectMapper.writer();
		String payloadJSON = objectWriter.writeValueAsString(requestPayload);

		Request request = new Request("POST", endpoint);
		request.setJsonEntity(payloadJSON);

		Response result = restClient.performRequest(request);
		InputStream inputStream = result.getEntity().getContent();
		String text = new BufferedReader(
			      new InputStreamReader(inputStream, StandardCharsets.UTF_8))
			        .lines()
			        .collect(Collectors.joining("\n"));
		return text;
	}
}