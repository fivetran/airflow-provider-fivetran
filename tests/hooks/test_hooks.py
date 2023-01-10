"""
Unittest module to test Fivetran Hooks.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:

    python3 -m unittest tests.hooks.test_hooks.TestFivetranHook

"""

import logging
import os
import pytest
import requests_mock
import unittest
from unittest import mock

# Import Hook
from fivetran_provider.hooks.fivetran import FivetranHook


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
        "paused": False,
        "pause_after_trial": False,
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
                            "column_1": {
                                "name_in_destination": "column_1",
                                "enabled": True,
                                "hashed": False,
                                "enabled_patch_settings": {
                                    "allowed": False,
                                    "reason_code": "SYSTEM_COLUMN",
                                    "reason": "The column does not support exclusion as it is a Primary Key",
                                },
                            },
                        },
                    }
                },
            }
        },
    },
}

MOCK_FIVETRAN_METADATA_TABLES_RESPONSE_PAYLOAD = {
    "code": "Success",
    "data": {
        "items": [
            {
                "id": "NjgyMDM0OQ",
                "parent_id": "ZGVtbw",
                "name_in_source": "subscription_periods",
                "name_in_destination": "subscription_periods",
            }
        ]
    },
}

MOCK_FIVETRAN_METADATA_COLUMNS_RESPONSE_PAYLOAD = {
    "code": "Success",
    "data": {
        "items": [
            {
                "id": "MjE0NDM2ODE2",
                "parent_id": "NjgyMDM0OQ",
                "name_in_source": "_file",
                "name_in_destination": "_file",
                "type_in_source": "String",
                "type_in_destination": "VARCHAR(256)",
                "is_primary_key": True,
                "is_foreign_key": False,
            },
        ]
    },
}

MOCK_FIVETRAN_DESTINATIONS_RESPONSE_PAYLOAD = {
    "code": "Success",
    "data": {
        "id": "rarer_gradient",
        "group_id": "rarer_gradient",
        "service": "google_sheets",
        "region": "GCP_US_EAST4",
        "time_zone_offset": "-8",
        "setup_status": "connected",
        "config": {"schema": "google_sheets.fivetran_google_sheets_spotify"},
    },
}

MOCK_FIVETRAN_GROUPS_RESPONSE_PAYLOAD = {
    "code": "Success",
    "data": {
        "id": "rarer_gradient",
        "name": "GoogleSheets",
        "created_at": "2022-12-12T17:14:33.790844Z",
    },
}


# Mock the `conn_fivetran` Airflow connection (note the `@` after `API_SECRET`)
@mock.patch.dict("os.environ", AIRFLOW_CONN_CONN_FIVETRAN="http://API_KEY:API_SECRET@")
class TestFivetranHook(unittest.TestCase):
    """
    Test functions for Fivetran Hook.

    Mocks responses from Fivetran API.
    """

    @requests_mock.mock()
    def test_get_connector(self, m):
        m.get(
            "https://api.fivetran.com/v1/connectors/interchangeable_revenge",
            json=MOCK_FIVETRAN_RESPONSE_PAYLOAD,
        )
        hook = FivetranHook(
            fivetran_conn_id="conn_fivetran",
        )
        result = hook.get_connector(connector_id="interchangeable_revenge")
        assert result["status"]["setup_state"] == "connected"

    @requests_mock.mock()
    def test_get_connector_schemas(self, m):
        m.get(
            "https://api.fivetran.com/v1/connectors/interchangeable_revenge/schemas",
            json=MOCK_FIVETRAN_SCHEMA_RESPONSE_PAYLOAD,
        )
        hook = FivetranHook(
            fivetran_conn_id="conn_fivetran",
        )
        result = hook.get_connector_schemas(connector_id="interchangeable_revenge")
        assert result["schemas"]["google_sheets.fivetran_google_sheets_spotify"][
            "enabled"
        ]

    @requests_mock.mock()
    def test_get_metadata_tables(self, m):
        m.get(
            "https://api.fivetran.com/v1/metadata/connectors/interchangeable_revenge/tables",
            json=MOCK_FIVETRAN_METADATA_TABLES_RESPONSE_PAYLOAD,
        )
        hook = FivetranHook(
            fivetran_conn_id="conn_fivetran",
        )
        result = hook.get_metadata(
            connector_id="interchangeable_revenge", metadata="tables"
        )
        assert result["items"][0]["id"] == "NjgyMDM0OQ"

    @requests_mock.mock()
    def test_get_metadata_columns(self, m):
        m.get(
            "https://api.fivetran.com/v1/metadata/connectors/interchangeable_revenge/columns",
            json=MOCK_FIVETRAN_METADATA_COLUMNS_RESPONSE_PAYLOAD,
        )
        hook = FivetranHook(
            fivetran_conn_id="conn_fivetran",
        )
        result = hook.get_metadata(
            connector_id="interchangeable_revenge", metadata="columns"
        )
        assert result["items"][0]["id"] == "MjE0NDM2ODE2"

    @requests_mock.mock()
    def test_get_destinations(self, m):
        m.get(
            "https://api.fivetran.com/v1/destinations/rarer_gradient",
            json=MOCK_FIVETRAN_DESTINATIONS_RESPONSE_PAYLOAD,
        )
        hook = FivetranHook(
            fivetran_conn_id="conn_fivetran",
        )
        result = hook.get_destinations(group_id="rarer_gradient")
        assert result["service"] == "google_sheets"

    @requests_mock.mock()
    def test_get_groups(self, m):
        m.get(
            "https://api.fivetran.com/v1/groups/rarer_gradient",
            json=MOCK_FIVETRAN_GROUPS_RESPONSE_PAYLOAD,
        )
        hook = FivetranHook(
            fivetran_conn_id="conn_fivetran",
        )
        result = hook.get_groups(group_id="rarer_gradient")
        assert result["id"] == "rarer_gradient"
        assert result["name"] == "GoogleSheets"

    @requests_mock.mock()
    def test_start_fivetran_sync(self, m):
        m.get(
            "https://api.fivetran.com/v1/connectors/interchangeable_revenge",
            json=MOCK_FIVETRAN_RESPONSE_PAYLOAD,
        )
        m.post(
            "https://api.fivetran.com/v1/connectors/interchangeable_revenge/force",
            json=MOCK_FIVETRAN_RESPONSE_PAYLOAD,
        )
        hook = FivetranHook(
            fivetran_conn_id="conn_fivetran",
        )
        result = hook.start_fivetran_sync(connector_id="interchangeable_revenge")
        assert result["code"] == "Success"


if __name__ == "__main__":
    unittest.main()
