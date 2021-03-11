from time import sleep
from urllib.parse import urlparse

import requests
from requests import PreparedRequest, exceptions as requests_exceptions
from requests.auth import AuthBase
import pendulum
import logging
import json
import time


from airflow import __version__
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook

log = logging.getLogger(__name__)

class FivetranHook(BaseHook):  # noqa
    """
    :param timeout_seconds: The amount of time in seconds the requests library
        will wait before timing-out.
    :type timeout_seconds: int
    :param retry_limit: The number of times to retry the connection in case of
        service outages.
    :type retry_limit: int
    :param retry_delay: The number of seconds to wait between retries (it
        might be a floating point number).
    :type retry_delay: float
    """

    conn_name_attr = 'fivetran_conn_id'
    conn_type = 'http'
    hook_name = 'Fivetran'

    def __init__(
        self,
        fivetran_conn_id: str = 'fivetran',
        timeout_seconds: int = 180,
        retry_limit: int = 3,
        retry_delay: float = 1.0,
    ) -> None:
        #super().__init__()
        self.fivetran_conn_id = fivetran_conn_id
        self.fivetran_conn = self.get_connection(fivetran_conn_id)
        self.timeout_seconds = timeout_seconds
        if retry_limit < 1:
            raise ValueError('Retry limit must be greater than equal to 1')
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay

    def _do_api_call(self, endpoint_info, json=None, force=''):
        """
        Utility function to perform an API call with retries
        :param endpoint_info: Tuple of method and endpoint
        :type endpoint_info: tuple[string, string]
        :param json: Parameters for this API call.
        :type json: dict
        :return: If the api call returns a OK status code,
            this function returns the response in JSON. Otherwise,
            we throw an AirflowException.
        :rtype: dict
        """
        method, connector_id = endpoint_info

        if 'token' in self.fivetran_conn.extra_dejson:
            self.log.info('Using token auth. ')
            auth = _TokenAuth(self.fivetran_conn.extra_dejson['token'])
            if 'host' in self.fivetran_conn.extra_dejson:
                host = self._parse_host(self.fivetran_conn.extra_dejson['host'])
            else:
                host = self.fivetran_conn.host
        else:
            self.log.info('Using basic auth. ')
            auth = (self.fivetran_conn.login, self.fivetran_conn.password)
            host = self.fivetran_conn.host

        url = host + connector_id + force

        if method == 'GET':
            request_func = requests.get
        elif method == 'POST':
            request_func = requests.post
        elif method == 'PATCH':
            request_func = requests.patch
        else:
            raise AirflowException('Unexpected HTTP Method: ' + method)

        attempt_num = 1
        while True:
            try:
                response = request_func(
                    url,
                    data=json if method == 'PATCH' else None,
                    params=json if method == 'GET' else None,
                    auth=auth,
                    headers={"Content-Type": "application/json;version=2"} if method == 'PATCH' else None,
                )
                response.raise_for_status()
                return response.json()
            except requests_exceptions.RequestException as e:
                if not _retryable_error(e):
                    # In this case, the user probably made a mistake.
                    # Don't retry.
                    raise AirflowException(
                        f'Response: {e.response.content}, Status Code: {e.response.status_code}'
                    )

                self._log_request_error(attempt_num, e)

            if attempt_num == self.retry_limit:
                raise AirflowException(
                    ('API requests to Fivetran failed {} times. ' + 'Giving up.').format(self.retry_limit)
                )

            attempt_num += 1
            sleep(self.retry_delay)

    def _log_request_error(self, attempt_num: int, error: str) -> None:
        self.log.error('Attempt %s API Request to Fivetran failed with reason: %s', attempt_num, error)

    def check_connector(self, connector_id):
        # Make sure connector configuration has been completed successfully and is not broken.
        
        resp = self._do_api_call(('GET',connector_id))
        connector_details = resp["data"]
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
        return resp

    def set_manual_schedule(self, connector_id):
        # Set connector to manual sync mode, required to force sync through the API
        return self._do_api_call(('PATCH',connector_id),json.dumps({"schedule_type": "manual"}))

    def start_fivetran_sync(self, connector_id):
        return self._do_api_call(('POST',connector_id),json=None, force='/force')

    def poll_fivetran_sync(self, connector_id, poll_frequency):
        #todo - replace below with Sensor
        
        resp = self._do_api_call(('GET',connector_id))
        connector_details = resp["data"]
        succeeded_at = self.parse_timestamp(connector_details["succeeded_at"])
        failed_at = self.parse_timestamp(connector_details["failed_at"])
        previous_completed_at = succeeded_at if succeeded_at > failed_at else failed_at
        

        loop: bool = True
        while loop:
            resp = self._do_api_call(('GET',connector_id))
            current_details = resp["data"]
            # Failsafe, in case we missed a state transition – it is possible with a long enough
            # `poll_frequency` we could completely miss the 'syncing' state
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
                        connector_id, URL_LOGS
                    )
                )
            # Started sync will spend some time in the 'scheduled' state before
            # transitioning to 'syncing'.
            # Capture the transition from 'scheduled' to 'syncing' or 'rescheduled',
            # and then back to 'scheduled' on completion.
            sync_state = current_details["status"]["sync_state"]
            log.info(
                'Connector "{}" current sync_state = {}'.format(
                    connector_id, sync_state
                )
            )
            if current_completed_at > previous_completed_at:
                loop = False
            else:
                time.sleep(poll_frequency)

        return {
            "succeeded_at": succeeded_at.to_iso8601_string(),
            "connector_id": connector_id,
        }

    def parse_timestamp(self, api_time):
        """Returns either the pendulum-parsed actual timestamp or
        a very out-of-date timestamp if not set
        """
        return (
            pendulum.parse(api_time)
            if api_time is not None
            else pendulum.from_timestamp(-1)
        )

def _retryable_error(exception) -> bool:
    return (
        isinstance(exception, (requests_exceptions.ConnectionError, requests_exceptions.Timeout))
        or exception.response is not None
        and exception.response.status_code >= 500
    )

class _TokenAuth(AuthBase):
    """
    Helper class for requests Auth field. AuthBase requires you to implement the __call__
    magic function.
    """

    def __init__(self, token: str) -> None:
        self.token = token

    def __call__(self, r: PreparedRequest) -> PreparedRequest:
        r.headers['Authorization'] = 'Bearer ' + self.token
        return r
