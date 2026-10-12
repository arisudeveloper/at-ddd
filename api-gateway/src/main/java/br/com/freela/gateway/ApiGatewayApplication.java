package br.com.freela.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableDiscoveryClient
public class ApiGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApiGatewayApplication.class, args);
    }

    @Bean
    public RouteLocator customRouteLocator(RouteLocatorBuilder builder) {
        return builder.routes()
                .route("contrato-service", r -> r.path("/api/contratos", "/api/contratos/**")
                        .uri("http://localhost:8081"))
                .route("notificacao-service", r -> r.path("/api/notificacoes", "/api/notificacoes/**")
                        .uri("http://localhost:8082"))
                .route("reputacao-service", r -> r.path("/api/reputacoes", "/api/reputacoes/**")
                        .uri("http://localhost:8083"))
                .route("auditoria-service", r -> r.path("/api/auditoria", "/api/auditoria/**")
                        .uri("http://localhost:8084"))
                .build();
    }
}