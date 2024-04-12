package dev.danielarrais.bigqueryintegrationtest.service;

import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.testing.RemoteBigQueryHelper;
import dev.danielarrais.bigqueryintegrationtest.config.BigQueryProperties;
import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.util.CollectionUtils;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Map;

@Log4j2
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class BigQueryEndToEndTest {

    String tableName = "teste";
    DatasetId datasetId;

    @Autowired
    private BigQueryService bigQueryService;

    @Autowired
    private BigQueryProperties bigQueryProperties;

    @Autowired
    private BigQuery bigQuery;

    @DynamicPropertySource
    static void setProperties(DynamicPropertyRegistry registry) {
        registry.add("gcp.bigquery.dataset", RemoteBigQueryHelper::generateDatasetName);
    }

    @BeforeEach
    public void setupDataLake() {
        createDataSet();
        createTable();
    }

    @AfterEach
    public void downDataLake() {
        clearDataLake();
    }

    @Test
    public void insertItemOnBigQueryTable() throws InterruptedException {
        var dataForInsert = dataForInsert();
        var tables = bigQueryService.listTables();
        var response = bigQueryService.insertData(dataForInsert);
        var data = bigQueryService.getData();

        Assertions.assertEquals(data.get(0), dataForInsert);
        Assertions.assertEquals(data.size(), 1);
        Assertions.assertEquals(tables.size(), 1);
        Assertions.assertTrue(CollectionUtils.isEmpty(response.getInsertErrors()));

        log.info("Tables: {}", tables);
        log.info("Insert result: {}", response);
        log.info("Data inserted: {}", data);
    }

    public Map<String, String> dataForInsert() {
        return Map.of("column_01", "teste 03", "column_02", "teste");
    }

    public void createDataSet() {
        var datasetInfo = DatasetInfo.newBuilder(bigQueryProperties.getDataset()).build();
        bigQuery.create(datasetInfo);
        datasetId = DatasetId.of(bigQueryProperties.getProjectId(), bigQueryProperties.getDataset());
        log.info("Dataset '{}' created", datasetId.getDataset());
    }

    public void createTable() {
        var schema = Schema.of(
                Field.of("column_01", StandardSQLTypeName.STRING),
                Field.of("column_02", StandardSQLTypeName.STRING));
        var tableId = TableId.of(bigQuery.getOptions().getProjectId(), bigQueryProperties.getDataset(), tableName);
        var tableDefinition = StandardTableDefinition.of(schema);
        var tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

        bigQuery.create(tableInfo);
        log.info("Table '{}' created", tableInfo.getTableId().getTable());
    }

    public void clearDataLake() {
        try {
            var datasetId = DatasetId.of(bigQueryProperties.getProjectId(), bigQueryProperties.getDataset());
            var deleted = bigQuery.delete(datasetId, BigQuery.DatasetDeleteOption.deleteContents());
            log.info("Dataset '{}' removed? {}", datasetId.getDataset(), deleted);
        } catch (Exception ignored) {}
    }
}
