package org.test.presto.jdbc;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClientBuilder;
import org.jsoup.Connection;
import org.jsoup.Connection.Method;
import org.jsoup.Connection.Response;
import org.jsoup.Jsoup;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class PrestoHttpTest {

	public static void main(String[] args) {
		try {
			new PrestoHttpTest().startHttpClient();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void startHttpClient() throws ClientProtocolException, UnsupportedEncodingException, IOException {
		HttpClient client = HttpClientBuilder.create().build();
		HttpResponse response = client.execute(this.createPost());
		String json = IOUtils.toString(response.getEntity().getContent(), "utf-8");
		JSONObject jsonObject = JSON.parseObject(json);
		String status = jsonObject.getJSONObject("stats").getString("state");
		while (!(status.equals("FINISHED") || status.equals("FAILED"))) {
			String nextUri = jsonObject.getString("nextUri");
			if ((nextUri == null) || nextUri.isEmpty()) {
				break;
			}
			jsonObject = this.executeNextRequest(client, nextUri);
			status = jsonObject.getJSONObject("stats").getString("state");
			if ("RUNNING".equals(status)) {
				this.printResult(jsonObject);
			}
		}
		if ("FAILED".equals(status)) {
			System.out.println(jsonObject.get("error"));
		}
	}

	private void printResult(JSONObject jsonObject) {
		System.out.println(jsonObject.get("columns"));
		System.out.println(jsonObject.get("data"));
	}

	private JSONObject executeNextRequest(HttpClient client, String nextUri) throws UnsupportedOperationException, IOException {
		HttpResponse response = client.execute(this.createNextGet(nextUri));
		String json = IOUtils.toString(response.getEntity().getContent(), "utf-8");

		return JSON.parseObject(json);
	}

	private HttpPost createPost() throws UnsupportedEncodingException {
		HttpPost post = new HttpPost("http://10.104.102.184:8888/v1/statement");
		post.setHeader("X-Presto-User", "presto");
		post.setHeader("X-Presto-Source", "zhaoshb");
		post.setHeader("X-Presto-Catalog", "hive2");
		post.setHeader("X-Presto-Schema", "bi_ods");
		post.setHeader("X-Presto-Time-Zone", "Asia/Shanghai");
		post.setHeader("X-Presto-Language", "zh-CN");
		post.setHeader("X-Presto-Transaction-Id", "NONE");
		post.setHeader("Content-Type", "application/octet-stream");
		String sql = "select * from sqlserver.dbo.t_b_cit limit 1000";
		post.setEntity(new ByteArrayEntity(sql.getBytes("utf-8"), ContentType.APPLICATION_OCTET_STREAM));

		return post;

	}

	private HttpGet createNextGet(String nextUri) {
		HttpGet httpGet = new HttpGet(nextUri);
		httpGet.setHeader("User-Agent", "StatementClient/0.154");
		httpGet.setHeader("X-Presto-User", "presto");

		return httpGet;
	}

	private void start() throws IOException {
		Connection conn = Jsoup.connect("http://10.104.102.184:8888/v1/statement");
		conn.requestBody("select * from hive2.bi_ods.t_scd_order_h limit 1000");
		conn.headers(this.createHeaders());
		Response response = conn.method(Method.POST).execute();
		System.out.println(response.headers());
		System.out.println(response.body());
	}

	private Map<String, String> createHeaders() {
		Map<String, String> headers = new HashMap<String, String>();
		headers.put("X-Presto-User", "presto");
		headers.put("X-Presto-Source", "zhaoshb");
		headers.put("X-Presto-Catalog", "hive2");
		headers.put("X-Presto-Schema", "bi_ods");
		headers.put("X-Presto-Time-Zone", "Asia/Shanghai");
		headers.put("X-Presto-Language", "zh-CN");
		headers.put("X-Presto-Transaction-Id", "NONE");
		headers.put("Content-Type", "application/octet-stream");

		return headers;
	}

}
