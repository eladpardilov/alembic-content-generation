from db_models.tables.configuration_table import ConfigurationsTable

table_content = [
    ConfigurationsTable(service="local_service1", version="v1",
                        hostname="localhost", port=8080, status="off"),
    ConfigurationsTable(service="local_service1", version="v2",
                        hostname="localhost", port=8080, status="on"),
    ConfigurationsTable(service="local_service2", version="v1",
                        hostname="localhost", port=8081, status="off"),
    ConfigurationsTable(service="local_service2", version="v2",
                        hostname="localhost", port=8081, status="on"),
    ConfigurationsTable(service="remote_service1", version="v1",
                        hostname="remotehost", port=8080, status="on"),
]
