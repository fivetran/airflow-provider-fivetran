"""
Unittest module to test Fivetran Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:

    python3 -m unittest tests.operators.test_operators.TestFivetranOperator

"""

import logging
import os
import pytest
import requests_mock
import unittest
from unittest import mock

# Import Operator as module
from fivetran_provider.operators.fivetran import FivetranOperator


log = logging.getLogger(__name__)

MOCK_FIVETRAN_RESPONSE_PAYLOAD = {
    "code": "Success",
    "data": {
        "id": "interchangeable_revenge",
        "group_id": "rarer_gradient",
        "service": "google_sheets",
        "service_version": 1,
        "schema": "google_sheets.fivetran_google_sheets_spotify",
        "connected_by": "mournful_shalt",
        "created_at": "2021-03-05T22:58:56.238875Z",
        "succeeded_at": "2021-03-23T20:55:12.670390Z",
        "failed_at": "null",
        "sync_frequency": 360,
        "schedule_type": "manual",
        "status": {
            "setup_state": "connected",
            "sync_state": "scheduled",
            "update_state": "on_schedule",
            "is_historical_sync": False,
            "tasks": [],
            "warnings": [],
        },
        "config": {
            "latest_version": "1",
            "sheet_id": "https://docs.google.com/spreadsheets/d/.../edit#gid=...",
            "named_range": "fivetran_test_range",
            "authorization_method": "User OAuth",
            "service_version": "1",
            "last_synced_changes__utc_": "2021-03-23 20:54",
        },
    },
}

MOCK_FIVETRAN_SCHEMA_RESPONSE_PAYLOAD = {
    "code": "Success",
    "data": {
        "enable_new_by_default": True,
        "schema_change_handling": "ALLOW_ALL",
        "schemas": {
            "google_sheets.fivetran_google_sheets_spotify": {
                "name_in_destination": "google_sheets.fivetran_google_sheets_spotify",
                "enabled": True,
                "tables": {
                    "table_1": {
                        "name_in_destination": "table_1",
                        "enabled": True,
                        "sync_mode": "SOFT_DELETE",
                        "enabled_patch_settings": {"allowed": True},
                        "columns": {
                            "column_1_": {
                                "name_in_destination": "column_1",
                                "enabled": True,
                                "hashed": False,
                                "enabled_patch_settings": {
                                    "allowed": False,
                                    "reason_code": "SYSTEM_COLUMN",
                                    "reason": "The column does not support exclusion as it is a Primary Key",
                                },
                            }
                        },
                    }
                },
            }
        },
    },
}


# Mock the `conn_fivetran` Airflow connection (note the `@` after `API_SECRET`)
@mock.patch.dict("os.environ", AIRFLOW_CONN_CONN_FIVETRAN="http://API_KEY:API_SECRET@")
class TestFivetranOperator(unittest.TestCase):
    """
    Test functions for Fivetran Operator.

    Mocks responses from Fivetran API.
    """

    @requests_mock.mock()
    def test_fivetran_operator(self, m):

        m.get(
            "https://api.fivetran.com/v1/connectors/interchangeable_revenge",
            json=MOCK_FIVETRAN_RESPONSE_PAYLOAD,
        )
        m.patch(
            "https://api.fivetran.com/v1/connectors/interchangeable_revenge",
            json=MOCK_FIVETRAN_RESPONSE_PAYLOAD,
        )
        m.post(
            "https://api.fivetran.com/v1/connectors/interchangeable_revenge/force",
            json=MOCK_FIVETRAN_RESPONSE_PAYLOAD,
        )

        operator = FivetranOperator(
            task_id="fivetran-task",
            fivetran_conn_id="conn_fivetran",
            connector_id="interchangeable_revenge",
        )

        result = operator.execute({})
        log.info(result)

        assert result["code"] == "Success"

    @requests_mock.mock()
    def test_fivetran_operator_get_openlineage_facets_on_complete(self, m):
        m.get(
            "https://api.fivetran.com/v1/connectors/interchangeable_revenge/schemas",
            json=MOCK_FIVETRAN_SCHEMA_RESPONSE_PAYLOAD,
        )
        m.get(
            "https://api.fivetran.com/v1/connectors/interchangeable_revenge",
            json=MOCK_FIVETRAN_RESPONSE_PAYLOAD,
        )

        operator = FivetranOperator(
            task_id="fivetran-task",
            fivetran_conn_id="conn_fivetran",
            connector_id="interchangeable_revenge",
        )

        facets = operator.get_openlineage_facets_on_complete(None)
        assert facets.inputs[0].facets["dataSource"].name == "fivetran"
        assert facets.inputs[0].name == "https://docs.google.com/spreadsheets/d/.../edit#gid=..."
        field = facets.outputs[0].facets["schema"].fields[0]
        assert field.name == "column_1"
        assert field.type == ""
        assert field.description is None

    @requests_mock.mock()
    def test_fivetran_operator_get_fields(self, m):
        m.get(
            "https://api.fivetran.com/v1/connectors/interchangeable_revenge/schemas",
            json=MOCK_FIVETRAN_SCHEMA_RESPONSE_PAYLOAD,
        )

        operator = FivetranOperator(
            task_id="fivetran-task",
            fivetran_conn_id="conn_fivetran",
            connector_id="interchangeable_revenge",
        )

        fields = operator._get_fields(
            MOCK_FIVETRAN_SCHEMA_RESPONSE_PAYLOAD["data"]["schemas"][
                "google_sheets.fivetran_google_sheets_spotify"
            ]["tables"]["table_1"]
        )
        log.info(fields)

        assert fields[0].name == "column_1"


if __name__ == "__main__":
    unittest.main()
