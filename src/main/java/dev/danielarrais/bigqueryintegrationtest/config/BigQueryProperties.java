package dev.danielarrais.bigqueryintegrationtest.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Getter
@Setter
@Configuration
@ConfigurationProperties("gcp.bigquery")
public class BigQueryProperties {
    private String credentialFile;
    private String location;
    private String projectId;
    private String host;
    private String dataset;
}
