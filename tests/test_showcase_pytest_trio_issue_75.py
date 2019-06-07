# Parsec Cloud (https://parsec.cloud) Copyright (c) AGPLv3 2019 Scille SAS

import pytest
import trio


@pytest.mark.trio
async def test_showcase_pytest_trio_issue_75(alice_core):
    print("Test started...")
    await trio.sleep(0.1)
    print("Test finished...")
