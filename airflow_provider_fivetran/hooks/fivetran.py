import json
from time import sleep

import requests
from requests import PreparedRequest, exceptions as requests_exceptions
from requests.auth import AuthBase
import pendulum

# from airflow import __version__
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook


class FivetranHook(BaseHook):
    """
    Fivetran API interaction hook.

    :param fivetran_conn_id: Maps to the id of the Connection to be used to
        configure this hook.
    :type fivetran_conn_id: str
    :param timeout_seconds: The amount of time in seconds the requests library
        will wait before timing-out.
    :type timeout_seconds: int
    :param retry_limit: The number of times to retry the connection in case of
        service outages.
    :type retry_limit: int
    :param retry_delay: The number of seconds to wait between retries.
    :type retry_delay: float
    """

    conn_name_attr = "fivetran_conn_id"
    conn_type = "http"
    hook_name = "Fivetran"

    def __init__(
        self,
        fivetran_conn_id: str = "fivetran",
        timeout_seconds: int = 180,
        retry_limit: int = 3,
        retry_delay: float = 1.0,
    ) -> None:
        super().__init__()
        self.fivetran_conn = self.get_connection(fivetran_conn_id)
        self.timeout_seconds = timeout_seconds
        if retry_limit < 1:
            raise ValueError("Retry limit must be greater than equal to 1")
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay

    def _do_api_call(self, endpoint_info, json=None, force=""):
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

        if "token" in self.fivetran_conn.extra_dejson:
            auth = _TokenAuth(self.fivetran_conn.extra_dejson["token"])
            if "host" in self.fivetran_conn.extra_dejson:
                host = self._parse_host(self.fivetran_conn.extra_dejson["host"])
            else:
                host = self.fivetran_conn.host
        else:
            auth = (self.fivetran_conn.login, self.fivetran_conn.password)
            host = self.fivetran_conn.host

        url = host + connector_id + force

        if method == "GET":
            request_func = requests.get
        elif method == "POST":
            request_func = requests.post
        elif method == "PATCH":
            request_func = requests.patch
        else:
            raise AirflowException("Unexpected HTTP Method: " + method)

        attempt_num = 1
        while True:
            try:
                response = request_func(
                    url,
                    data=json if method in ("POST", "PATCH") else None,
                    params=json if method in ("GET") else None,
                    auth=auth,
                    headers={"Content-Type": "application/json;version=2"}
                    if method == "PATCH"
                    else None,
                )
                response.raise_for_status()
                return response.json()
            except requests_exceptions.RequestException as e:
                if not _retryable_error(e):
                    # In this case, the user probably made a mistake.
                    # Don't retry.
                    raise AirflowException(
                        f"Response: {e.response.content}, "
                        f"Status Code: {e.response.status_code}"
                    )

                self._log_request_error(attempt_num, e)

            if attempt_num == self.retry_limit:
                raise AirflowException(
                    f"API request to Fivetran failed {self.retry_limit} times."
                    " Giving up."
                )

            attempt_num += 1
            sleep(self.retry_delay)

    def _log_request_error(self, attempt_num: int, error: str) -> None:
        self.log.error(
            "Attempt %s API Request to Fivetran failed with reason: %s",
            attempt_num,
            error,
        )

    def _connector_ui_url(self, service_name, schema_name):
        return (
            f"https://fivetran.com/dashboard/connectors/"
            f"{service_name}/{schema_name}"
        )

    def _connector_ui_url_logs(self, service_name, schema_name):
        return self._connector_ui_url(service_name, schema_name) + "/logs"

    def _connector_ui_url_setup(self, service_name, schema_name):
        return self._connector_ui_url(service_name, schema_name) + "/setup"

    def check_connector(self, connector_id):
        """
        Ensures connector configuration has been completed successfully and is in
            a functional state.

        :param connector_id: Fivetran connector_id, found in connector settings
            page in the Fivetran user interface.
        :type connector_id: str
        """
        resp = self._do_api_call(("GET", connector_id))
        connector_details = resp["data"]
        service_name = connector_details["service"]
        schema_name = connector_details["schema"]
        setup_state = connector_details["status"]["setup_state"]

        if setup_state != "connected":
            raise AirflowException(
                f'Fivetran connector "{self.connector_id}" not correctly configured, '
                f"status: {setup_state}\nPlease see: "
                f"{self._connector_ui_url_setup(service_name, schema_name)}"
            )
        # URL for connector logs within the UI
        self.log.info(
            f"Connector type: {service_name}, connector schema: {schema_name}"
        )
        self.log.info(
            f"Connectors logs at "
            f"{self._connector_ui_url_logs(service_name, schema_name)}"
        )
        return resp

    def set_manual_schedule(self, connector_id):
        """
        Set connector to manual sync mode, required to force sync through the API.
            Syncs will no longer be performed automatically and must be started
            via the API.

        :param connector_id: Fivetran connector_id, found in connector settings
            page in the Fivetran user interface.
        :type connector_id: str
        """
        return self._do_api_call(
            ("PATCH", connector_id), json.dumps({"schedule_type": "manual"})
        )

    def start_fivetran_sync(self, connector_id):
        """
        :param connector_id: Fivetran connector_id, found in connector settings
            page in the Fivetran user interface.
        :type connector_id: str
        """
        return self._do_api_call(("POST", connector_id), json=None, force="/force")

    def poll_fivetran_sync(self, connector_id, poll_frequency=60):
        """
        Returns if the connector is not syncing or if the connector is syncing,
            when the connector has completed its sync; in any other case, raises
            an exception

        :param connector_id: Fivetran connector_id, found in connector settings
            page in the Fivetran user interface.
        :type connector_id: str
        :param poll_frequency: delay in seconds to poll the API for status updates
        :type connector_id: int
        """
        # todo - replace below with Sensor
        resp = self._do_api_call(("GET", connector_id))
        connector_details = resp["data"]
        service_name = connector_details["service"]
        schema_name = connector_details["schema"]
        previous_sync_state = connector_details["status"]["sync_state"]
        succeeded_at = self._parse_timestamp(connector_details["succeeded_at"])
        failed_at = self._parse_timestamp(connector_details["failed_at"])
        previous_completed_at = succeeded_at if succeeded_at > failed_at else failed_at

        # @todo Need logic here to tell if the sync is not running at all and not
        # likely to run in the near future.

        loop: bool = True
        while loop:
            resp = self._do_api_call(("GET", connector_id))
            current_details = resp["data"]
            # Failsafe, in case we missed a state transition – it is possible
            # with a long enough `poll_frequency` we could completely miss the
            # 'syncing' state
            succeeded_at = self._parse_timestamp(current_details["succeeded_at"])
            failed_at = self._parse_timestamp(current_details["failed_at"])
            current_completed_at = (
                succeeded_at if succeeded_at > failed_at else failed_at
            )
            # The only way to tell if a sync failed is to check if its latest
            # failed_at value is greater than then last known "sync completed at" value.
            if failed_at > previous_completed_at:
                raise AirflowException(
                    f'Fivetran sync for connector "{connector_id}" failed; '
                    f"please see logs at "
                    f"{self._connector_ui_url_logs(service_name, schema_name)}"
                )
            # Started sync will spend some time in the 'scheduled' state before
            # transitioning to 'syncing'.
            # Capture the transition from 'scheduled' to 'syncing' or 'rescheduled',
            # and then back to 'scheduled' on completion.
            sync_state = current_details["status"]["sync_state"]
            if sync_state != previous_sync_state:
                previous_sync_state = sync_state
                self.log.info(f'Connector "{connector_id}": sync_state = {sync_state}')
            if current_completed_at > previous_completed_at:
                loop = False
            else:
                sleep(poll_frequency)

        return {
            "succeeded_at": succeeded_at.to_iso8601_string(),
            "connector_id": connector_id,
        }

    def _parse_timestamp(self, api_time):
        """
        Returns either the pendulum-parsed actual timestamp or
            a very out-of-date timestamp if not set

        :param api_time: timestamp format as returned by the Fivetran API.
        :type api_time: str
        :rtype: Pendulum.DateTime
        """
        return (
            pendulum.parse(api_time)
            if api_time is not None
            else pendulum.from_timestamp(-1)
        )


def _retryable_error(exception) -> bool:
    return (
        isinstance(
            exception,
            (requests_exceptions.ConnectionError, requests_exceptions.Timeout),
        )
        or exception.response is not None
        and exception.response.status_code >= 500
    )


class _TokenAuth(AuthBase):
    """
    Helper class for requests Auth field. AuthBase requires you to implement the
        __call__ magic function.
    """

    def __init__(self, token: str) -> None:
        self.token = token

    def __call__(self, r: PreparedRequest) -> PreparedRequest:
        r.headers["Authorization"] = "Bearer " + self.token
        return r
