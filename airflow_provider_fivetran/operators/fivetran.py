from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import Optional

from airflow_provider_fivetran.hooks.fivetran import FivetranHook


class FivetranOperator(BaseOperator):
    """
    FivetranOperator starts the sync job of a Fivetran connector, and will
    exit when the sync is complete or raise an exception otherwise.

    :param fivetran_conn_id: Connection ID as specified in Airflow settings
    :type fivetran_conn_id: Optional[str]
    :param fivetran_retry_limit: # of retries when encountering API errors
    :type fivetran_retry_limit: Optional[int]
    :param fivetran_retry_delay: Time to wait before retrying API request
    :type fivetran_retry_delay: int
    :param connector_id: ID of the Fivetran connector to sync, found on the
        Connector settings page.
    :type connector_id: str
    :param poll_frequency: In seconds. A lower value means more frequent API polling
        for sync status; 3 seconds is about the minimum before hitting rate limits.
    :type poll_frequency: Optional[int]
    """

    @apply_defaults
    def __init__(
        self,
        run_name: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        fivetran_conn_id: str = 'fivetran',
        fivetran_retry_limit: int = 3,
        fivetran_retry_delay: int = 1,
        connector_id: str = None,
        poll_frequency: int = 15,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.fivetran_conn_id = fivetran_conn_id
        self.fivetran_retry_limit = fivetran_retry_limit
        self.fivetran_retry_delay = fivetran_retry_delay
        self.connector_id = connector_id
        self.poll_frequency = poll_frequency

    def _get_hook(self) -> FivetranHook:
        return FivetranHook(
            self.fivetran_conn_id,
            retry_limit=self.fivetran_retry_limit,
            retry_delay=self.fivetran_retry_delay,
        )

    def execute(self, context):
        hook = self._get_hook()
        hook.check_connector(self.connector_id)
        hook.set_manual_schedule(self.connector_id)
        hook.start_fivetran_sync(self.connector_id)
        hook.poll_fivetran_sync(self.connector_id, self.poll_frequency)
