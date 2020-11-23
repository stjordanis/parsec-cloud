# Parsec Cloud (https://parsec.cloud) Copyright (c) AGPLv3 2019 Scille SAS

import pytest
from unittest.mock import ANY
from uuid import uuid4
from pendulum import now as pendulum_now

from tests.backend.common import organization_stats


@pytest.mark.trio
async def test_organization_stats(
    running_backend,
    backend,
    realm,
    alice,
    alice_backend_sock,
    bob_backend_sock,
    otheralice_backend_sock,
):
    rep = await organization_stats(alice_backend_sock)
    assert rep == {"status": "ok", "users": 3, "metadata_size": ANY, "data_size": 0}
    initial_metadata_size = rep["metadata_size"]

    # Create new metadata
    await backend.vlob.create(
        organization_id=alice.organization_id,
        author=alice.device_id,
        encryption_revision=1,
        timestamp=pendulum_now(),
        realm_id=realm,
        vlob_id=uuid4(),
        blob=b"1234",
    )
    rep = await organization_stats(alice_backend_sock)
    assert rep == {
        "status": "ok",
        "users": 3,
        "metadata_size": initial_metadata_size + 4,
        "data_size": 0,
    }

    # Create new data
    await backend.block.create(
        organization_id=alice.organization_id,
        author=alice.device_id,
        realm_id=realm,
        block_id=uuid4(),
        block=b"1234",
    )
    rep = await organization_stats(alice_backend_sock)
    assert rep == {
        "status": "ok",
        "users": 3,
        "metadata_size": initial_metadata_size + 4,
        "data_size": 4,
    }

    # Bob is not admin, it should fail
    rep = await organization_stats(bob_backend_sock)
    assert rep == {"status": "not_allowed", "reason": "User `bob` is not admin"}

    # Ensure organization isolation
    other_stats = await organization_stats(otheralice_backend_sock)
    assert other_stats == {"status": "ok", "users": 1, "metadata_size": ANY, "data_size": 0}
