from airflow.models import BaseOperator, BaseOperatorLink
from airflow.utils.decorators import apply_defaults

from fivetran_provider.hooks.fivetran import FivetranHook
from openlineage.airflow.extractors.base import BaseExtractor, OperatorLineage
from openlineage.common.dataset import Dataset, Field, Source
from typing import Optional


class RegistryLink(BaseOperatorLink):
    """Link to Registry"""

    name = "Astronomer Registry"

    def get_link(self, operator, dttm):
        """Get link to registry page."""

        registry_link = (
            "https://registry.astronomer.io/providers/{provider}/modules/{operator}"
        )
        return registry_link.format(provider="fivetran", operator="fivetranoperator")


class FivetranOperator(BaseOperator):
    """
    `FivetranOperator` starts a Fivetran sync job.

    `FivetranOperator` requires that you specify the `connector_id` of the sync job to
    start. You can find `connector_id` in the Settings page of the connector you
    configured in the `Fivetran dashboard <https://fivetran.com/dashboard/connectors>`_.
    Note that when a Fivetran sync job is controlled via an Operator, it is no longer
    run on the schedule as managed by Fivetran. In other words, it is now scheduled only
    from Airflow. This can be changed with the schedule_type parameter.

    :param fivetran_conn_id: `Conn ID` of the Connection to be used to configure
        the hook.
    :type fivetran_conn_id: Optional[str]
    :param fivetran_retry_limit: # of retries when encountering API errors
    :type fivetran_retry_limit: Optional[int]
    :param fivetran_retry_delay: Time to wait before retrying API request
    :type fivetran_retry_delay: int
    :param connector_id: ID of the Fivetran connector to sync, found on the
        Connector settings page.
    :type connector_id: str
    :param schedule_type: schedule type. Default is "manual" which takes the connector off Fivetran schedule. Set to "auto" to keep connector on Fivetran schedule.
    :type schedule_type: str
    """

    operator_extra_links = (RegistryLink(),)

    # Define which fields get jinjaified
    template_fields = ["connector_id"]

    @apply_defaults
    def __init__(
        self,
        connector_id: str,
        run_name: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        fivetran_conn_id: str = "fivetran",
        fivetran_retry_limit: int = 3,
        fivetran_retry_delay: int = 1,
        poll_frequency: int = 15,
        schedule_type: str = "manual",
        **kwargs
    ):
        super().__init__(**kwargs)
        self.fivetran_conn_id = fivetran_conn_id
        self.fivetran_retry_limit = fivetran_retry_limit
        self.fivetran_retry_delay = fivetran_retry_delay
        self.connector_id = connector_id
        self.poll_frequency = poll_frequency
        self.schedule_type = schedule_type

    def _get_hook(self) -> FivetranHook:
        return FivetranHook(
            self.fivetran_conn_id,
            retry_limit=self.fivetran_retry_limit,
            retry_delay=self.fivetran_retry_delay,
        )

    def execute(self, context):
        hook = self._get_hook()
        hook.prep_connector(self.connector_id, self.schedule_type)
        return hook.start_fivetran_sync(self.connector_id)

    def _get_fields(self, table) -> Optional[Field]:
        if table.get("columns"):
            return [
                Field(
                    name=col["name_in_destination"],
                    type="",
                )
                for col in table["columns"].values()
            ]
        return None

    def _get_input_name(self, config, service) -> str:
        if service == "gcs":
            return f"{config['bucket']}/{config['prefix']}{config['pattern']}"
        elif service == "google_sheets":
            return config['sheet_id']
        else:
            raise ValueError(f"Service: {service} not supported by extractor.")

    def get_openlineage_facets_on_complete(self, task_instance) -> OperatorLineage:
        facets = OperatorLineage()
        hook = self._get_hook()
        connector_resp = hook.get_connector(self.connector_id)
        schema_resp = hook.get_connector_schemas(self.connector_id)
        config = connector_resp["config"]
        input_name = self._get_input_name(config, connector_resp["service"])
        inputs = []
        outputs = []

        for schema in schema_resp["schemas"].values():

            source = Source(
                scheme="fivetran",
                authority="",
                connection_url=BaseExtractor.get_connection_uri(hook.fivetran_conn),
            )

            inputs.append(
                Dataset(
                    source=source,
                    name=input_name,
                )
            )

            outputs.extend([
                Dataset(
                    source=source,
                    name=table["name_in_destination"],
                    fields=self._get_fields(table),
                )
                for table in schema["tables"].values()
            ])

        job_facets = {}
        run_facets = {}

        facets.inputs = [ds.to_openlineage_dataset() for ds in inputs]
        facets.outputs = [ds.to_openlineage_dataset() for ds in outputs]
        facets.job_facets = job_facets
        facets.run_facets = run_facets
        return facets
