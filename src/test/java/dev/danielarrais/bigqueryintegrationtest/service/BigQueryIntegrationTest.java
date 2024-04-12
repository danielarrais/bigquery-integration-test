package dev.danielarrais.bigqueryintegrationtest.service;

import com.google.cloud.bigquery.*;
import dev.danielarrais.bigqueryintegrationtest.config.BigQueryProperties;
import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.util.CollectionUtils;
import org.testcontainers.containers.BigQueryEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Map;

@Log4j2
@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class BigQueryIntegrationTest {

    @Container
    static BigQueryEmulatorContainer bigQueryContainer = new BigQueryEmulatorContainer("ghcr.io/goccy/bigquery-emulator:0.4.3");
    String tableName = "teste";

    @Autowired
    private BigQueryService bigQueryService;

    @Autowired
    private BigQueryProperties bigQueryProperties;

    @Autowired
    private BigQuery bigQuery;

    @BeforeEach
    public void setupDataLake() {
        clearDataSet();
        createDataSet();
        createTable();
    }

    @DynamicPropertySource
    static void setProperties(DynamicPropertyRegistry registry) {
        registry.add("gcp.bigquery.project-id", bigQueryContainer::getProjectId);
        registry.add("gcp.bigquery.host", bigQueryContainer::getEmulatorHttpEndpoint);
        registry.add("gcp.bigquery.dataset", () -> "local-bigquery");
    }

    @Test
    public void insertItemOnBigQueryTable() throws InterruptedException {
        var dataForInsert = dataForInsert();
        var response = bigQueryService.insertData(dataForInsert);
        var data = bigQueryService.getData();
        var tables = bigQueryService.listTables();

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
        log.info("Dataset '{}' created", bigQueryProperties.getDataset());
    }

    public void createTable() {
        var schema = Schema.of(
                Field.of("column_01", StandardSQLTypeName.STRING),
                Field.of("column_02", StandardSQLTypeName.STRING));
        var tableId = TableId.of(bigQueryProperties.getDataset(), tableName);
        var tableDefinition = StandardTableDefinition.of(schema);
        var tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

        bigQuery.create(tableInfo);
        log.info("Table '{}' created", tableInfo.getTableId().getTable());
    }

    public void clearDataSet() {
        bigQuery.delete(getDatasetId());
        log.info("Dataset '{}' removed", bigQueryProperties.getDataset());
    }

    private DatasetId getDatasetId() {
        return DatasetId.of(bigQueryContainer.getProjectId(), bigQueryProperties.getDataset());
    }
}
