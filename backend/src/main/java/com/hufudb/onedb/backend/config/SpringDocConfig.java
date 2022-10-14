package com.hufudb.onedb.backend.config;

import io.swagger.v3.oas.models.ExternalDocumentation;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.springdoc.core.GroupedOpenApi;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SpringDocConfig {
  @Bean
  public OpenAPI mallTinyOpenAPI() {
    return new OpenAPI()
        .info(new Info().title("Hufu Backend API")
            .description("Hufu Backend API")
            .version("v1.0.0")
            .license(new License().name("Apache 2.0").url("https://raw.githubusercontent.com/BUAA-BDA/Hu-Fu/main/LICENSE")))
        .externalDocs(new ExternalDocumentation()
            .description("Hufu documents")
            .url("https://github.com/BUAA-BDA/Hu-Fu/blob/main/README.md"));
  }

  @Bean
  public GroupedOpenApi ownerApi() {
    return GroupedOpenApi.builder()
        .group("hufu-doc-v1")
        .pathsToMatch("/**")
        .build();
  }

}