package com.thibaultleouay.asyncbigqueryclient;

import static com.google.common.util.concurrent.MoreExecutors.getExitingExecutorService;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetList;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.JobCancelResponse;
import com.google.api.services.bigquery.model.JobList;
import com.google.api.services.bigquery.model.ProjectList;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableDataList;
import com.google.api.services.bigquery.model.TableList;
import com.google.api.services.bigquery.model.JobList.Jobs;

import io.netty.handler.codec.http.HttpMethod;

public class BigQueryImpl implements BigQuery {

	private static final String BIG_QUERY = "https://www.googleapis.com/bigquery/v2";
	private final ExecutorService executor = getExitingExecutorService(
			(ThreadPoolExecutor) Executors.newCachedThreadPool());
	AsyncHttpClient client;
	private GoogleCredential credential;

	BigQueryImpl() {

		client = new DefaultAsyncHttpClient();

		Collection<String> bigqueryScopes = BigqueryScopes.all();

		// instances of HTTP transport and JSON factory objects.
		HttpTransport transport = new NetHttpTransport();
		JsonFactory jsonFactory = new JacksonFactory();

		credential = null;
		try {
			credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if ((credential).createScopedRequired()) {
			credential = (credential).createScoped(bigqueryScopes);
		}
	}

	public BigQueryFuture<Object> read() {

		return null;
	}

	@Override
	public BigQueryFuture<Dataset> getDataset(String projectId, String datasetId) {

		return get("get Datasets", URLUtil.generateDatasetsUrl(projectId, datasetId), Dataset.class);
	}

	public BigQueryFuture<Dataset> insertDataset(String projectId, Dataset datasets) {
		return post("insert Dataset", URLUtil.generateDatasetsUrl(projectId, ""), datasets, Dataset.class);
	}

	@Override
	public BigQueryFuture<Dataset> deleteDataset(String projectId, String datasetId) {
		return delete("delete Datasets", URLUtil.generateDatasetsUrl(projectId, datasetId), Dataset.class);

	}

	@Override
	public BigQueryFuture<DatasetList> listDataset(String projectId) {
		return get("get List Datasets", URLUtil.generateDatasetsUrl(projectId, ""), DatasetList.class);

	}

	@Override
	public BigQueryFuture<Dataset> patchDataset(String projectId, String datasetId, Dataset datasets) {
		return patch("patch Dataset", URLUtil.generateDatasetsUrl(projectId, datasetId), datasets, Dataset.class);

	}

	@Override
	public BigQueryFuture<Dataset> updateDataset(String projectId, String datasetId, Dataset datasets) {
		return put("update Dataset", URLUtil.generateDatasetsUrl(projectId, datasetId), datasets, Dataset.class);

	}

	@Override
	public BigQueryFuture<Jobs> getJob(String projectId, String jobId) {
		return get("get job", URLUtil.generateJobUrl(projectId, jobId), Jobs.class);
	}

	@Override
	public BigQueryFuture<JobCancelResponse> cancelJob(String projectId, String jobId) {
		return post("cancel job", URLUtil.generateJobUrl(projectId, jobId) + "/cancel", null, JobCancelResponse.class);
	}

	@Override
	public BigQueryFuture<GetQueryResultsResponse> getQueryResult(String projectId, String jobId) {
		return get("get query result", URLUtil.generateQueryUrl(projectId, jobId), GetQueryResultsResponse.class);
	}

	@Override
	public BigQueryFuture<JobList> listJobs(String projectId) {
		return get("list all jobs", URLUtil.generateJobUrl(projectId, ""), JobList.class);
	}

	@Override
	public BigQueryFuture<QueryResponse> queryJob(String projectId) {
		return post("query job", URLUtil.generateQueryUrl(projectId, ""), null, QueryResponse.class);
	}

	@Override
	public BigQueryFuture<ProjectList> listProject() {
		return get("list Projects", "/projects", ProjectList.class);
	}

	@Override
	public BigQueryFuture<TableDataInsertAllResponse> insertAllTabledata(String projectId, String datasetId,
			String tableId, TableDataInsertAllRequest payload) {
		return post("insert All tabledata", URLUtil.generateTableURL(projectId, datasetId, tableId) + "/insertAll",
				payload, TableDataInsertAllResponse.class);
	}

	@Override
	public BigQueryFuture<TableDataList> listTabledata(String projectId, String datasetId, String tableId) {
		return get("get list tableData", URLUtil.generateTableURL(projectId, datasetId, tableId) + "/data",
				TableDataList.class);
	}

	@Override
	public BigQueryFuture<Void> deleteTable(String projectId, String datasetId, String tableId) {
		return delete("delete table", URLUtil.generateTableURL(projectId, datasetId, tableId), Void.class);
	}

	@Override
	public BigQueryFuture<Table> getTable(String projectId, String datasetId, String tableId) {
		return get("get table", URLUtil.generateTableURL(projectId, datasetId, tableId), Table.class);
	}

	@Override
	public BigQueryFuture<Table> insertTable(String projectId, String datasetId, String tableId, Table payload) {
		return post("insert Table", URLUtil.generateTableURL(projectId, datasetId, ""), payload, Table.class);
	}

	@Override
	public BigQueryFuture<TableList> listTable(String projectId, String datasetId) {
		return get("insert Table", URLUtil.generateTableURL(projectId, datasetId, ""), TableList.class);
	}

	@Override
	public BigQueryFuture<Table> patchTable(String projectId, String datasetId, String tableId, Table payload) {
		return patch("insert Table", URLUtil.generateTableURL(projectId, datasetId, tableId), payload, Table.class);
	}

	@Override
	public BigQueryFuture<Table> updateTable(String projectId, String datasetId, String tableId, Table payload) {
		return put("insert Table", URLUtil.generateTableURL(projectId, datasetId, tableId), payload, Table.class);
	}

	private <T> BigQueryFuture<T> delete(final String operation, final String path, final Class<T> responseClass) {
		return request(operation, HttpMethod.DELETE, path, responseClass, null);
	}

	private <T> BigQueryFuture<T> get(final String operation, final String path, final Class<T> responseClass) {
		return request(operation, HttpMethod.GET, path, responseClass, null);
	}

	private <T> BigQueryFuture<T> patch(final String operation, final String path, final Object payload,
			final Class<T> responseClass) {
		return request(operation, HttpMethod.PATCH, path, responseClass, payload);
	}

	private <T> BigQueryFuture<T> post(final String operation, final String path, final Object payload,
			final Class<T> responseClass) {
		return request(operation, HttpMethod.POST, path, responseClass, payload);
	}

	private <T> BigQueryFuture<T> put(final String operation, final String path, final Object payload,
			final Class<T> responseClass) {
		return request(operation, HttpMethod.PUT, path, responseClass, payload);
	}

	private <T> BigQueryFuture<T> request(final String operation, final HttpMethod method, final String path,
			final Class<T> responseClass, final Object payload) {

		final RequestBuilder builder = new RequestBuilder().setUrl(BIG_QUERY + path).setMethod(method.toString())
				.setHeader("Authorization", "Bearer " + credential.getAccessToken()).setHeader("User-Agent", "toto");

		if (payload != null) {
			builder.setHeader("Content-Type", "application/json; charset=UTF-8");
			try {
				builder.setBody(JacksonFactory.getDefaultInstance().toString(payload));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		Request request = builder.build();

		BigQueryFuture<T> future = new BigQueryFuture<>();
		client.executeRequest(request, new AsyncHandler<Void>() {
			private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

			@Override
			public void onThrowable(final Throwable t) {
				future.fail(t);
			}

			@Override
			public Void onCompleted() throws Exception {
				if (future.isDone()) {
					return null;
				}
				System.out.println(bytes.toString());
				future.succeed(JacksonFactory.getDefaultInstance().fromString(bytes.toString(), responseClass));
				// future.succeed(gson.fromJson(bytes.toString(), responseClass));

				return null;
			}

			@Override
			public State onBodyPartReceived(org.asynchttpclient.HttpResponseBodyPart bodyPart) throws Exception {
				bytes.write(bodyPart.getBodyPartBytes());
				return State.CONTINUE;
			}

			@Override
			public State onStatusReceived(org.asynchttpclient.HttpResponseStatus responseStatus) throws Exception {
				// Return null for 404'd GET & DELETE requests
				if (responseStatus.getStatusCode() == 404 && method == HttpMethod.GET || method == HttpMethod.DELETE) {
					future.succeed(null);
					return State.ABORT;
				}

				// Fail on non-2xx responses
				final int statusCode = responseStatus.getStatusCode();
				if (!(statusCode >= 200 && statusCode < 300)) {
					future.fail(
							new RequestFailedException(responseStatus.getStatusCode(), responseStatus.getStatusText()));
					return State.ABORT;
				}

				if (responseClass == Void.class) {
					future.succeed(null);
					return State.ABORT;
				}

				return State.CONTINUE;
			}

			@Override
			public State onHeadersReceived(org.asynchttpclient.HttpResponseHeaders headers) throws Exception {
				return State.CONTINUE;
			}
		});

		return future;
	}

	@Override
	public void close() throws IOException {
		client.close();
	}
}
