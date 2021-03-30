from typing import Any

from airflow.exceptions import AirflowException
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

from fivetran_provider.hooks.fivetran import FivetranHook


class FivetranSensor(BaseSensorOperator):
    """
    `FivetranSensor` monitors a Fivetran sync job for completion.

    Monitoring with `FivetranSensor` allows you to trigger downstream processes only
    when the Fivetran sync jobs have completed, ensuring data consistency. You can
    use multiple instances of `FivetranSensor` to monitor multiple Fivetran
    connectors. Note, it is possible to monitor a sync that is scheduled and managed
    from Fivetran; in other words, you can use `FivetranSensor` without using
    `FivetranOperator`. If used in this way, your DAG will wait until the sync job
    starts on its Fivetran-controlled schedule and then completes. `FivetranSensor`
    requires that you specify the `connector_id` of the sync job to start. You can
    find `connector_id` in the Settings page of the connector you configured in the
    `Fivetran dashboard <https://fivetran.com/dashboard/connectors>`_.


    :param fivetran_conn_id: `Conn ID` of the Connection to be used to configure
        the hook.
    :type fivetran_conn_id: str
    :param connector_id: ID of the Fivetran connector to sync, found on the
        Connector settings page in the Fivetran Dashboard.
    :type connector_id: str
    :param poke_interval: Time in seconds that the job should wait in
        between each tries
    :type poke_interval: int
    """

    # Define which fields get jinjaified
    template_fields = ["connector_id"]

    @apply_defaults
    def __init__(
        self,
        fivetran_conn_id: str = 'fivetran',
        poke_interval: int = 60,
        connector_id=None,
        **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self.fivetran_conn_id = fivetran_conn_id
        self.connector_id = connector_id
        self.poke_interval = poke_interval
        self.hook = None
        self.previous_completed_at = None

    def poke(self, context):
        if self.hook is None:
            self.hook = FivetranHook(self.fivetran_conn_id)
        if self.previous_completed_at is None:
            self.previous_completed_at = self.hook.get_last_sync(self.connector_id)
        return self.hook.get_sync_status(self.connector_id, self.previous_completed_at)
