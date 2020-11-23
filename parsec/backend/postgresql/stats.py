# Parsec Cloud (https://parsec.cloud) Copyright (c) AGPLv3 2019 Scille SAS

from uuid import UUID

from parsec.api.protocol import UserID, DeviceID, OrganizationID
from parsec.backend.stats import BaseStatsComponent, OrganizationStats, UserStats, DeviceStats
from parsec.backend.postgresql.handler import PGHandler
from parsec.backend.postgresql.utils import (
    query,
    Q,
    q_organization_internal_id,
    q_user_internal_id,
    q_device_internal_id,
)


# _q_get_organization_stats = Q(
#     f"""
#     SELECT
#         crunched_on,
#         user_count,
#         block_size,
#         block_count,
#         vlob_size,
#         vlob_count
#     FROM organization_stats
#     WHERE
#         organization = { q_organization_internal_id("$organization_id") }
#         AND crunched_on > (now() - (interval '1 second' * $max_age))
#     """
# )


# _q_crunch_organization_stats = Q(
#     f"""
# INSERT INTO organization_stats(
#     organization,
#     crunched_on,
#     user_count,
#     block_size,
#     block_count,
#     vlob_size,
#     vlob_count
# )
# VALUES(
#     { q_organization_internal_id("$organization_id") },
#     now(),
#     (
#         SELECT COUNT(*)
#         FROM user_
#         WHERE user_.organization = { q_organization_internal_id("$organization_id") }
#     ),
#     (
#         SELECT COALESCE(SUM(size), 0)
#         FROM block
#         WHERE
#             organization = { q_organization_internal_id("$organization_id") }
#     ),
#     (
#         SELECT COUNT(*)
#         FROM block
#         WHERE
#             organization = { q_organization_internal_id("$organization_id") }
#     ),
#     (
#         SELECT COALESCE(SUM(size), 0)
#         FROM vlob_atom
#         WHERE
#             organization = { q_organization_internal_id("$organization_id") }
#     ),
#     (
#         SELECT COUNT(*)
#         FROM vlob_atom
#         WHERE
#             organization = { q_organization_internal_id("$organization_id") }
#     )
# )
# ON CONFLICT (organization)
# DO UPDATE SET
#     crunched_on = EXCLUDED.crunched_on,
#     user_count = EXCLUDED.user_count,
#     block_size = EXCLUDED.block_size,
#     block_count = EXCLUDED.block_count,
#     vlob_size = EXCLUDED.vlob_size,
#     vlob_count = EXCLUDED.vlob_count
# RETURNING
#     crunched_on,
#     user_count,
#     block_size,
#     block_count,
#     vlob_size,
#     vlob_count
# """
# )


_q_update_device_stats = Q(
    f"""
INSERT INTO device_stats(
    device,
    last_connected_on,
    block_size,
    block_count,
    vlob_size,
    vlob_count
)
VALUES(
    { q_device_internal_id(organization_id="$organization_id", device_id="$device_id") },
    now(),
    $block_size,
    $block_count,
    $vlob_size,
    $vlob_count
)
ON CONFLICT (device)
DO UPDATE SET
    last_connected_on = last_connected_on + EXCLUDED.last_connected_on
    block_size = block_size + EXCLUDED.block_size,
    block_count = block_count + EXCLUDED.block_count,
    vlob_size = vlob_size + EXCLUDED.vlob_size,
    vlob_count = vlob_count + EXCLUDED.vlob_count
"""
)


_q_get_organization_stats = Q(
    f"""
SELECT
    MAX(last_connected_on) last_connected_on,
    (
        SELECT COUNT(*)
        FROM user_
        WHERE user_.organization = { q_organization_internal_id("$organization_id") }
    ) user_count,
    SUM(block_size) block_size,
    SUM(block_count) block_count,
    SUM(vlob_size) vlob_size,
    SUM(vlob_count) vlob_count
FROM device_stats
LEFT JOIN device
ON
    device_stats.device = device._id
WHERE
    device.organization = { q_organization_internal_id("$organization_id") }
"""
)


_q_get_user_stats = Q(
    f"""
SELECT
    MAX(last_connected_on) last_connected_on,
    SUM(*) device_count,
    SUM(block_size) block_size,
    SUM(block_count) block_count,
    SUM(vlob_size) vlob_size,
    SUM(vlob_count) vlob_count
FROM device_stats
LEFT JOIN device
ON
    device_stats.device = device._id
WHERE
    device.user_ = { q_user_internal_id(organization_id="$organization_id", user_id="$user_id") }
"""
)


_q_get_device_stats = Q(
    f"""
SELECT
    last_connected_on,
    block_size,
    block_count,
    vlob_size,
    vlob_count
FROM device_stats
WHERE
    _id = { q_device_internal_id(organization_id="$organization_id", device_id="$device_id") }
"""
)


@query(in_transaction=True)
async def query_update_device_stats(
    conn,
    organization_id: OrganizationID,
    device_id: DeviceID,
    block_size: int = 0,
    block_count: int = 0,
    vlob_size: int = 0,
    vlob_count: int = 0,
) -> None:
    await conn.execute(
        *_q_update_device_stats(
            organization_id=organization_id,
            device_id=device_id,
            block_size=block_size,
            block_count=block_count,
            vlob_size=vlob_size,
            vlob_count=vlob_count,
        )
    )


class PGStatsComponent(BaseStatsComponent):
    def __init__(self, dbh: PGHandler, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dbh = dbh

    async def organization_stats(self, organization_id: OrganizationID) -> OrganizationStats:
        async with self.dbh.pool.acquire() as conn:
            row = await conn.fetchrow(*_q_get_organization_stats(organization_id=organization_id))
            return OrganizationStats(
                organization_id=organization_id,
                last_connected_on=row["last_connected_on"],
                user_count=row["user_count"],
                device_count=row["device_count"],
                block_size=row["block_size"],
                block_count=row["block_count"],
                vlob_size=row["vlob_size"],
                vlob_count=row["vlob_count"],
            )

    async def user_stats(self, organization_id: OrganizationID, user_id: UserID) -> UserStats:
        async with self.dbh.pool.acquire() as conn:
            row = await conn.fetchrow(
                *_q_get_user_stats(organization_id=organization_id, user_id=user_id)
            )
            return UserStats(
                organization_id=organization_id,
                user_id=user_id,
                last_connected_on=row["last_connected_on"],
                device_count=row["devices_count"],
                block_size=row["block_size"],
                block_count=row["block_count"],
                vlob_size=row["vlob_size"],
                vlob_count=row["vlob_count"],
            )

    async def device_stats(
        self, organization_id: OrganizationID, device_id: DeviceID
    ) -> DeviceStats:
        async with self.dbh.pool.acquire() as conn:
            row = await conn.fetchrow(
                *_q_get_device_stats(organization_id=organization_id, device_id=device_id)
            )
            return DeviceStats(
                organization_id=organization_id,
                device_id=device_id,
                last_connected_on=row["last_connected_on"],
                block_size=row["block_size"],
                block_count=row["block_count"],
                vlob_size=row["vlob_size"],
                vlob_count=row["vlob_count"],
            )

    async def realm_stats(self, organization_id: OrganizationID, workspace_id: UUID):
        pass

    async def update_last_connection(
        self, organization_id: OrganizationID, device_id: DeviceID
    ) -> None:
        async with self.dbh.pool.acquire() as conn:
            await query_update_device_stats(conn, organization_id, device_id)
