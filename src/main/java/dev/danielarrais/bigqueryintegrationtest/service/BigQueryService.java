package dev.danielarrais.bigqueryintegrationtest.service;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.*;
import dev.danielarrais.bigqueryintegrationtest.config.BigQueryProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

@Service
@RequiredArgsConstructor
public class BigQueryService {
    String tableName = "teste";

    private final BigQuery bigquery;
    private final BigQueryProperties properties;

    public InsertAllResponse insertData(Map<String, String> data) {
        var tableId = TableId.of(properties.getProjectId(), properties.getDataset(), tableName);
        var contentForInsert = InsertAllRequest
                .newBuilder(tableId)
                .addRow(data).build();

        return bigquery.insertAll(contentForInsert);
    }

    public List<HashMap<String, String>> getData() throws InterruptedException {
        var fullTableName = String.format("%s.%s.%s", properties.getProjectId(), properties.getDataset(), tableName);
        var query = "SELECT * FROM `" + fullTableName + "` LIMIT 100";

        var results = bigquery.query(QueryJobConfiguration.of(query));
        var schemaResult = results.getSchema();

        return results.streamValues().map(fieldValues -> {
            var map = new HashMap<String, String>();
            IntStream.range(0, fieldValues.size()).forEach(index -> {
                var columnName = schemaResult.getFields().get(index).getName();
                map.put(columnName, fieldValues.get(index).getStringValue());
            });
            return map;
        }).toList();
    }

    public List<String> listTables() {
        DatasetId datasetId = DatasetId.of(bigquery.getOptions().getProjectId(), properties.getDataset());
        Page<Table> tables = bigquery.listTables(datasetId, BigQuery.TableListOption.pageSize(100));
        return tables.streamValues().map(table -> table.getTableId().getTable()).toList();
    }
}
