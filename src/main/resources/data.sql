INSERT INTO app_config (config_key, config_value)
VALUES ('queue_listener_enabled', 'true')
ON CONFLICT (config_key) DO NOTHING;
