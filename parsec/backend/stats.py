# Parsec Cloud (https://parsec.cloud) Copyright (c) AGPLv3 2019 Scille SAS

import attr
from uuid import UUID
from pendulum import DateTime

from parsec.api.protocol import UserID, DeviceID, OrganizationID


@attr.s(slots=True, frozen=True, auto_attribs=True)
class OrganizationStats:
    last_connected_on: DateTime
    user_count: int
    device_count: int
    block_size: int
    block_count: int
    vlob_size: int
    vlob_count: int


@attr.s(slots=True, frozen=True, auto_attribs=True)
class UserStats:
    last_connected_on: DateTime
    device_count: int
    block_size: int
    block_count: int
    vlob_size: int
    vlob_count: int


@attr.s(slots=True, frozen=True, auto_attribs=True)
class DeviceStats:
    last_connected_on: DateTime
    block_size: int
    block_count: int
    vlob_size: int
    vlob_count: int


@attr.s(slots=True, frozen=True, auto_attribs=True)
class RealmStats:
    block_size: int
    block_count: int
    vlob_size: int
    vlob_count: int


class StatsError(Exception):
    pass


class BaseStatsComponent:
    async def organization_stats(self, organization: OrganizationID) -> OrganizationStats:
        """
        Raises: Nothing !
        """
        raise NotImplementedError

    async def user_stats(self, user: UserID) -> UserStats:
        """
        Raises: Nothing !
        """
        raise NotImplementedError

    async def device_stats(self, device: DeviceID) -> DeviceStats:
        """
        Raises: Nothing !
        """
        raise NotImplementedError

    async def realm_stats(self, organization_id: OrganizationID, realm_id: UUID) -> RealmStats:
        """
        Raises: Nothing !
        """
        raise NotImplementedError

    async def update_last_connection(self, device: DeviceID, now: DateTime = None) -> None:
        """
        Raises: Nothing !
        """
        raise NotImplementedError
