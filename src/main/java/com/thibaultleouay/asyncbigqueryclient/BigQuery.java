package com.thibaultleouay.asyncbigqueryclient;

import java.io.Closeable;

import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetList;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.JobCancelResponse;
import com.google.api.services.bigquery.model.JobList;
import com.google.api.services.bigquery.model.ProjectList;
import com.google.api.services.bigquery.model.JobList.Jobs;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableDataList;
import com.google.api.services.bigquery.model.TableList;


public interface BigQuery extends Closeable {

	public static BigQuery create(BigQueryBuilder builder) {
		return new BigQueryImpl();
		}
	
	// Datasets
	public BigQueryFuture<Dataset> getDataset(String projectId, String datasetId);
	
	public BigQueryFuture<Dataset> insertDataset(String projectId, Dataset datasets);
	
	public BigQueryFuture<Dataset> deleteDataset(String projectId, String datasetId);
	
	public BigQueryFuture<DatasetList> listDataset(String projectId);
	
	public BigQueryFuture<Dataset> patchDataset(String projectId, String datasetId, Dataset datasets);
	
	public BigQueryFuture<Dataset> updateDataset(String projectId, String datasetId, Dataset datasets);

	
	// Jobs
	public BigQueryFuture<JobCancelResponse> cancelJob(String projectId, String jobId);
	
	public BigQueryFuture<Jobs> getJob(String projectId, String jobId);
	
	public BigQueryFuture<GetQueryResultsResponse> getQueryResult(String projectId, String jobId);
	
	public BigQueryFuture<JobList> listJobs(String projectId);
	
	public BigQueryFuture<QueryResponse> queryJob(String projectId);
	
	
	// Projects
	public BigQueryFuture<ProjectList> listProject();
	
	
	// Tabledata 
	public BigQueryFuture<TableDataInsertAllResponse> insertAllTabledata(String projectId, String datasetId, String tableId, TableDataInsertAllRequest payload);
	
	public BigQueryFuture<TableDataList> listTabledata(String projectId, String datasetId, String tableId);
	
	
	// Tables
	public BigQueryFuture<Void> deleteTable(String projectId, String datasetId, String tableId);
	
	public BigQueryFuture<Table> getTable(String projectId, String datasetId, String tableId);
	
	public BigQueryFuture<Table> insertTable(String projectId, String datasetId, String tableId, Table payload);
	
	public BigQueryFuture<TableList> listTable(String projectId, String datasetId);
	
	public BigQueryFuture<Table> patchTable(String projectId, String datasetId, String tableId, Table payload);
	
	public BigQueryFuture<Table> updateTable(String projectId, String datasetId, String tableId, Table payload);
}
