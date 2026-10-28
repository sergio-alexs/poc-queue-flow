package com.example.demo_colas_azure.repository;

import java.util.Optional;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import com.example.demo_colas_azure.entity.AppConfig;

@Repository
public interface AppConfigRepository extends CrudRepository<AppConfig, Long> {

    Optional<AppConfig> findByConfigKey(String configKey);
}
