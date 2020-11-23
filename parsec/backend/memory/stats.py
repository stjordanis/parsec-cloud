# Parsec Cloud (https://parsec.cloud) Copyright (c) AGPLv3 2019 Scille SAS

from typing import Dict, Tuple
from uuid import UUID
from pendulum import DateTime, now as pendulum_now

from parsec.api.protocol import UserID, DeviceID, OrganizationID
from parsec.backend.stats import (
    BaseStatsComponent,
    OrganizationStats,
    UserStats,
    DeviceStats,
    RealmStats,
)
from parsec.backend import memory


class MemoryStatsComponent(BaseStatsComponent):
    def __init__(self):
        self._user_component: "memory.MemoryUserComponent"  # Defined in `register_components`
        self._vlob_component: "memory.MemoryVlobComponent"  # Defined in `register_components`
        self._block_component: "memory.MemoryBlockComponent"  # Defined in `register_components`
        self._last_connections: Dict[Tuple[OrganizationID, DeviceID], DateTime] = {}

    def register_components(
        self,
        user: "memory.MemoryUserComponent",
        vlob: "memory.MemoryVlobComponent",
        block: "memory.MemoryBlockComponent",
        **other_components,
    ):
        self._user_component = user
        self._vlob_component = vlob
        self._block_component = block

    async def organization_stats(self, organization_id: OrganizationID) -> OrganizationStats:
        return OrganizationStats(
            last_connected_on=max(
                [
                    dt
                    for (orgid, _), dt in self._last_connections.items()
                    if orgid == organization_id
                ]
            ),
            **self._user_component.stats(organization_id=organization_id),
            **self._vlob_component.stats(organization_id=organization_id),
            **self._block_component.stats(organization_id=organization_id),
        )

    async def user_stats(self, organization_id: OrganizationID, user_id: UserID) -> UserStats:
        return UserStats(
            last_connected_on=max(
                [
                    dt
                    for (orgid, devid), dt in self._last_connections.items()
                    if orgid == organization_id and devid.user_id == user_id
                ]
            ),
            **self._user_component.stats(organization_id=organization_id, user_id=user_id),
            **self._vlob_component.stats(organization_id=organization_id, user_id=user_id),
            **self._block_component.stats(organization_id=organization_id, user_id=user_id),
        )

    async def device_stats(
        self, organization_id: OrganizationID, device_id: DeviceID
    ) -> DeviceStats:
        return DeviceStats(
            last_connected_on=self._last_connections[organization_id, device_id],
            **self._vlob_component.stats(organization_id=organization_id, device_id=device_id),
            **self._block_component.stats(organization_id=organization_id, device_id=device_id),
        )

    async def realm_stats(self, organization_id: OrganizationID, realm_id: UUID) -> RealmStats:
        return RealmStats(
            **self._vlob_component.stats(organization_id=organization_id, realm_id=realm_id),
            **self._block_component.stats(organization_id=organization_id, realm_id=realm_id),
        )

    async def update_last_connection(
        self, organization_id: OrganizationID, device_id: DeviceID
    ) -> None:
        self._last_connections[organization_id, device_id] = pendulum_now()
