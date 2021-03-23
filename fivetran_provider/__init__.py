def get_provider_info():
  return {
      "package-name": "airflow-provider-fivetran",
      "name": "Fivetran Provider",
      "description": "An Apache Airflow Provider for Fivetran.",
      "hook-class-names": ["fivetran_provider.hooks.fivetran.FivetranHook"],
      "extra-links":["fivetran_provider.operators.fivetran.RegistryLink"],
      "versions": ["0.1.0"]
  }