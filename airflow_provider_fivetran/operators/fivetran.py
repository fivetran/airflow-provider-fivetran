import logging
import json
import time

import requests
import pendulum

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

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
        api_key=None,
        api_secret=None,
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
        self.api_key = api_key
        self.api_secret = api_secret
        self.connector_id = connector_id
        self.poll_status_every_n_seconds = poll_status_every_n_seconds
        super().__init__(**kwargs)

        if not self.connector_id:
            raise ValueError("Value for parameter `connector_id` must be provided.")
        if not self.api_key:
            raise ValueError("Value for parameter `api_key` must be provided.")
        if not self.api_secret:
            raise ValueError("Value for parameter `api_secret` must be provided.")

    def parse_timestamp(self, api_time):
        """Returns either the pendulum-parsed actual timestamp or
        a very out-of-date timestamp if not set
        """
        return (
            pendulum.parse(api_time)
            if api_time is not None
            else pendulum.from_timestamp(-1)
        )

    def execute(self, context):

        URL_CONNECTOR: str = "https://api.fivetran.com/v1/connectors/{}".format(
            self.connector_id
        )

        log.info(
            "Attempting start of Fivetran connector {}, sleep time set to {} seconds.".format(
                self.connector_id, self.poll_status_every_n_seconds
            )
        )

        # Automatically call `raise_for_status` on every request
        session = requests.Session()
        session.hooks = {"response": lambda r, *args, **kwargs: r.raise_for_status()}
        # Make sure connector configuration has been completed successfully and is not broken.
        resp = session.get(URL_CONNECTOR, auth=(self.api_key, self.api_secret))
        connector_details = resp.json()["data"]
        URL_LOGS = "https://fivetran.com/dashboard/connectors/{}/{}/logs".format(
            connector_details["service"], connector_details["schema"]
        )
        URL_SETUP = "https://fivetran.com/dashboard/connectors/{}/{}/setup".format(
            connector_details["service"], connector_details["schema"]
        )
        setup_state = connector_details["status"]["setup_state"]
        if setup_state != "connected":
            EXC_SETUP: str = (
                'Fivetran connector "{}" not correctly configured, status: {}; '
                + "please complete setup at {}"
            )
            raise ValueError(
                EXC_SETUP.format(self.connector_id, setup_state, URL_SETUP)
            )
        # We need to know the previous job's completion time to know if the job succeeded or failed
        succeeded_at = self.parse_timestamp(connector_details["succeeded_at"])
        failed_at = self.parse_timestamp(connector_details["failed_at"])
        previous_completed_at = succeeded_at if succeeded_at > failed_at else failed_at
        # URL for connector logs within the UI
        log.info(
            "Connector type: {}, connector schema: {}".format(
                connector_details["service"], connector_details["schema"]
            )
        )
        log.info("Connectors logs at {}".format(URL_LOGS))

        # Set connector to manual sync mode, required to force sync through the API
        resp = session.patch(
            URL_CONNECTOR,
            data=json.dumps({"schedule_type": "manual"}),
            headers={"Content-Type": "application/json;version=2"},
            auth=(self.api_key, self.api_secret),
        )
        # Start connector sync
        resp = session.post(
            URL_CONNECTOR + "/force", auth=(self.api_key, self.api_secret)
        )

        loop: bool = True
        while loop:
            resp = session.get(URL_CONNECTOR, auth=(self.api_key, self.api_secret))
            current_details = resp.json()["data"]
            # Failsafe, in case we missed a state transition – it is possible with a long enough
            # `poll_status_every_n_seconds` we could completely miss the 'syncing' state
            succeeded_at = self.parse_timestamp(current_details["succeeded_at"])
            failed_at = self.parse_timestamp(current_details["failed_at"])
            current_completed_at = (
                succeeded_at if succeeded_at > failed_at else failed_at
            )
            # The only way to tell if a sync failed is to check if its latest failed_at value
            # is greater than then last known "sync completed at" value.
            if failed_at > previous_completed_at:
                raise ValueError(
                    'Fivetran sync for connector "{}" failed; please see logs at {}'.format(
                        self.connector_id, URL_LOGS
                    )
                )
            # Started sync will spend some time in the 'scheduled' state before
            # transitioning to 'syncing'.
            # Capture the transition from 'scheduled' to 'syncing' or 'rescheduled',
            # and then back to 'scheduled' on completion.
            sync_state = current_details["status"]["sync_state"]
            log.info(
                'Connector "{}" current sync_state = {}'.format(
                    self.connector_id, sync_state
                )
            )
            if current_completed_at > previous_completed_at:
                loop = False
            else:
                time.sleep(self.poll_status_every_n_seconds)

        return {
            "succeeded_at": succeeded_at.to_iso8601_string(),
            "connector_id": self.connector_id,
        }
