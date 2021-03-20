def get_provider_info():
  return {
      "package-name": "airflow-provider-fivetran",
      "name": "Fivetran Provider",
      "description": "An Apache Airflow Provider for Fivetran.",
      "hook-class-names": ["airflow_provider_fivetran.hooks.fivetran.FivetranHook"],
      "extra-links":["airflow_provider_fivetran.operators.fivetran.RegistryLink"],
      "versions": ["0.1.0"]
  }