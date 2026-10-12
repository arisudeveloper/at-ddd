package br.com.freela.contrato;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ContratoApplication {

    public static void main(String[] args) {
        System.setProperty("spring.datasource.url", "jdbc:postgresql://localhost:5432/postgres");
        System.setProperty("spring.datasource.username", "freela");
        System.setProperty("spring.datasource.password", "freela");

        SpringApplication.run(ContratoApplication.class, args);
    }
}