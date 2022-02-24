from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, BaseOperatorLink
from airflow.utils.decorators import apply_defaults

from fivetran_provider.hooks.fivetran import FivetranHook
import time
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
    from Airflow.

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
    :param manual: manual schedule flag, Default is true, to take connector off Fivetran schedule. Set to false to disable and keep connector on Fivetran schedule
    :type manual: bool
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
        manual: bool = True,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.fivetran_conn_id = fivetran_conn_id
        self.fivetran_retry_limit = fivetran_retry_limit
        self.fivetran_retry_delay = fivetran_retry_delay
        self.connector_id = connector_id
        self.poll_frequency = poll_frequency
        self.manual = manual

    def _get_hook(self) -> FivetranHook:
        return FivetranHook(
            self.fivetran_conn_id,
            retry_limit=self.fivetran_retry_limit,
            retry_delay=self.fivetran_retry_delay,
        )

    def execute(self, context):
        hook = self._get_hook()
        hook.prep_connector(self.connector_id, self.manual)
        return hook.start_fivetran_sync(self.connector_id)


class FivetranPatientOperator(BaseOperator):
    """
    `FivetranPatientOperator` starts a Fivetran sync job and waits until
    it is completed or timeout limit is succeeded.

    `FivetranPatientOperator` requires that you specify the `connector_id` of the sync job to
    start. You can find `connector_id` in the Settings page of the connector you
    configured in the `Fivetran dashboard <https://fivetran.com/dashboard/connectors>`_.
    Note that when a Fivetran sync job is controlled via an Operator, it is no longer
    run on the schedule as managed by Fivetran. In other words, it is now scheduled only
    from Airflow.

    :param fivetran_conn_id: `Conn ID` of the Connection to be used to configure
        the hook.
    :type fivetran_conn_id: Optional[str]
    :param timeout_seconds: Timeout of polling sync
    :type timeout_seconds: int
    :param fivetran_retry_limit: # of retries when encountering API errors
    :type fivetran_retry_limit: Optional[int]
    :param fivetran_retry_delay: Time to wait before retrying API request
    :type fivetran_retry_delay: int
    :param connector_id: ID of the Fivetran connector to sync, found on the
        Connector settings page.
    :type connector_id: str
    :param poll_frequency: Frequency to poll (in seconds) to see if the sync
        is completed.
    :type poll_frequency: int
    :param manual: manual schedule flag, Default is true, to take connector off Fivetran schedule. Set to false to disable and keep connector on Fivetran schedule
    :type manual: bool
    """

    operator_extra_links = (RegistryLink(),)

    # Define which fields get jinjaified
    template_fields = ["connector_id"]

    @apply_defaults
    def __init__(
        self,
        connector_id: str,
        run_name: Optional[str] = None,
        timeout_seconds: int = 300,
        fivetran_conn_id: str = "fivetran",
        fivetran_retry_limit: int = 3,
        fivetran_retry_delay: int = 1,
        poll_frequency: int = 15,
        manual: bool = True,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.timeout_seconds = timeout_seconds
        self.fivetran_conn_id = fivetran_conn_id
        self.fivetran_retry_limit = fivetran_retry_limit
        self.fivetran_retry_delay = fivetran_retry_delay
        self.connector_id = connector_id
        self.poll_frequency = poll_frequency
        self.manual = manual
        self.previous_completed_at = None

    def _get_hook(self) -> FivetranHook:
        return FivetranHook(
            self.fivetran_conn_id,
            retry_limit=self.fivetran_retry_limit,
            retry_delay=self.fivetran_retry_delay,
        )

    def execute(self, context):
        hook = self._get_hook()
        hook.prep_connector(self.connector_id, self.manual)
        hook.start_fivetran_sync(self.connector_id)

        self.log.info(f'Connector "{self.connector_id}": Sync triggered')

        timeout = time.time() + self.timeout_seconds

        sync_status = False
        while not sync_status and time.time() < timeout:
            if self.previous_completed_at is None:
                self.previous_completed_at = hook.get_last_sync(self.connector_id)
            sync_status = hook.get_sync_status(self.connector_id, self.previous_completed_at)
            time.sleep(self.poll_frequency)

        if not sync_status:
            raise AirflowException(
                f'Timeout exceeded. Marking connector "{self.connector_id}" as failed')
        else:
            self.log.info(f'Connector "{self.connector_id}": Sync complete')
            return sync_status
