from __future__ import annotations

import os

from dotenv import load_dotenv
import pytest

from tests.integration.support import IntegrationSource


@pytest.fixture(scope="session")
def integration_source() -> IntegrationSource:
    load_dotenv()

    return IntegrationSource(
        project=os.environ["INTEGRATION_GCP_PROJECT"],
        source_bucket=os.environ[
            "INTEGRATION_SOURCE_BUCKET"
        ],
        source_object=os.environ[
            "INTEGRATION_SOURCE_OBJECT"
        ],
        source_generation=int(
            os.environ[
                "INTEGRATION_SOURCE_GENERATION"
            ]
        ),
    )
