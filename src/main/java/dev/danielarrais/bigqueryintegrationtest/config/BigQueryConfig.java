package dev.danielarrais.bigqueryintegrationtest.config;

import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.util.StringUtils;

import java.io.FileInputStream;
import java.io.IOException;

import static com.google.auth.oauth2.ServiceAccountCredentials.fromStream;

@Configuration
@RequiredArgsConstructor
public class BigQueryConfig {

    private final BigQueryProperties properties;

    @Bean
    @Primary
    @Profile("!test")
    public BigQuery bigQuery() throws IOException {
        if (StringUtils.hasText(properties.getCredentialFile())) {
            var credentialFile = new FileInputStream(properties.getCredentialFile());
            return BigQueryOptions.newBuilder()
                    .setCredentials(fromStream(credentialFile))
                    .build().getService();
        } else {
            throw new RuntimeException("No GCP credential file provided");
        }
    }

    @Bean
    @Profile("test")
    public BigQuery bigQueryTest() {
        return BigQueryOptions.newBuilder()
                .setHost(properties.getHost())
                .setLocation(properties.getLocation())
                .setCredentials(NoCredentials.getInstance())
                .setProjectId(properties.getProjectId())
                .build().getService();
    }
}
