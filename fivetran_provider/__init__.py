def get_provider_info():
  return {
      "package-name": "airflow-provider-fivetran",
      "name": "Fivetran Provider",
      "description": "A Fivetran provider for Apache Airflow.",
      "hook-class-names": ["fivetran_provider.hooks.fivetran.FivetranHook"],
      "extra-links":["fivetran_provider.operators.fivetran.RegistryLink"],
      "versions": ["1.0.1"]
  }
