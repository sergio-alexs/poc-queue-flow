package com.example.demo_colas_azure.entity;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Table("app_config")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class AppConfig {

    @Id
    private Long id;

    private String configKey;

    private String configValue;
}
