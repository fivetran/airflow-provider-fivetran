import logging
import json
import time

import requests

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import Any, Dict, List, Optional, Union


from hooks.fivetran import FivetranHook

# from airflow.plugins_manager import AirflowPlugin


log = logging.getLogger(__name__)


class FivetranOperator(BaseOperator):
    """
    FivetranOperator starts the sync job of a Fivetran connector, and will
    exit when the sync is complete or raise an exception otherwise.

    :param str api_key: Fivetran API key, found in Account Settings
    :param str api_secret: Fivetran API secret, found in Account Settings.
    :param str connector_id: ID of the Fivetran connector to sync, found on the Connector settings page.
    :param int poll_status_every_n_seconds: A lower value means more frequent API polling for sync
                status; 3 seconds is about the minimum before hitting rate limits.
    """

    @apply_defaults
    def __init__(
        self,
        run_name: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        fivetran_conn_id: str = 'fivetran',
        fivetran_retry_limit: int = 3,
        fivetran_retry_delay: int = 1,
        connector_id=None,
        poll_status_every_n_seconds: int = 15,
        **kwargs
    ):
        """
        An invocation of `run` will attempt to start a sync job for the specified `connector_id`. `run`
        will poll Fivetran for connector status, and will only complete when the sync has completed or
        when it receives an error status code from an API call.
        Args:
            - api_key (str): `API key` per https://fivetran.com/account/settings; should be secret!
            - api_secret (str): `API secret` per https://fivetran.com/account/settings; should be secret!
            - connector_id (str, optional): if provided, will overwrite value provided at init.
            - poll_status_every_n_seconds (int, optional): this task polls the Fivetran API for status,
                if provided this value will override the default polling time of 15 seconds.
        Returns:
            connector_id (str) and succeeded_at (timestamp str)
        """
        

        super().__init__(**kwargs)
        self.fivetran_conn_id = fivetran_conn_id
        self.fivetran_retry_limit = fivetran_retry_limit
        self.fivetran_retry_delay = fivetran_retry_delay
        self.connector_id = connector_id
        self.poll_status_every_n_seconds = poll_status_every_n_seconds

    def _get_hook(self) -> FivetranHook:
        return FivetranHook(
            self.fivetran_conn_id,
            retry_limit=self.fivetran_retry_limit,
            retry_delay=self.fivetran_retry_delay,
        )

    def execute(self, context):
        hook = self._get_hook()
        resp = hook.check_connector(self.connector_id)
        resp = hook.set_manual_schedule(self.connector_id)
        resp = hook.start_fivetran_sync(self.connector_id)
        resp = hook.poll_fivetran_sync(self.connector_id, self.poll_status_every_n_seconds)
