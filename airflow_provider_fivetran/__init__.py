def get_provider_info():
  return {
      "package-name": "airflow-provider-fivetran",
      "name": "Fivetran Provider",
      "description": "An Apache Airflow Provider for Fivetran.",
      "hook-class-names": ["airflow_provider_fivetran.hooks.fivetran.FivetranHook"],
      "versions": ["0.0.1"]
  }