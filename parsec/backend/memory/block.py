# Parsec Cloud (https://parsec.cloud) Copyright (c) AGPLv3 2019 Scille SAS

from uuid import UUID
import attr
from typing import Dict

from parsec.api.protocol import UserID, DeviceID, OrganizationID
from parsec.api.protocol import RealmRole
from parsec.backend.realm import RealmNotFoundError
from parsec.backend.blockstore import BaseBlockStoreComponent
from parsec.backend.block import (
    BaseBlockComponent,
    BlockAlreadyExistsError,
    BlockAccessError,
    BlockNotFoundError,
    BlockInMaintenanceError,
)
from parsec.backend import memory


@attr.s(auto_attribs=True)
class BlockMeta:
    realm_id: UUID
    author: DeviceID
    size: int


class MemoryBlockComponent(BaseBlockComponent):
    def __init__(self):
        self._blockstore_component: BaseBlockStoreComponent  # Defined in `register_components`
        self._realm_component: "memory.MemoryRealmComponent"  # Defined in `register_components`
        self._blockmetas = {}

    def register_components(
        self,
        blockstore: BaseBlockStoreComponent,
        realm: "memory.MemoryRealmComponent",
        **other_components,
    ):
        self._blockstore_component = blockstore
        self._realm_component = realm

    def stats(
        self,
        organization_id: OrganizationID,
        user_id: UserID = None,
        device_id: DeviceID = None,
        realm_id: UUID = None,
    ) -> Dict:
        block_count = 0
        block_size = 0
        for (org_id, _), meta in self._blockmetas.items():
            if org_id != organization_id:
                continue
            if user_id is not None and meta.author.user_id != user_id:
                continue
            if device_id is not None and meta.author != device_id:
                continue
            if realm_id is not None and meta.realm_id != realm_id:
                continue
            block_count += 1
            block_size += meta.size
        return {"block_count": block_count, "block_size": block_size}

    def _check_realm_read_access(self, organization_id, realm_id, user_id):
        can_read_roles = (
            RealmRole.OWNER,
            RealmRole.MANAGER,
            RealmRole.CONTRIBUTOR,
            RealmRole.READER,
        )
        self._check_realm_access(organization_id, realm_id, user_id, can_read_roles)

    def _check_realm_write_access(self, organization_id, realm_id, user_id):
        can_write_roles = (RealmRole.OWNER, RealmRole.MANAGER, RealmRole.CONTRIBUTOR)
        self._check_realm_access(organization_id, realm_id, user_id, can_write_roles)

    def _check_realm_access(self, organization_id, realm_id, user_id, allowed_roles):
        try:
            realm = self._realm_component._get_realm(organization_id, realm_id)
        except RealmNotFoundError:
            raise BlockNotFoundError(f"Realm `{realm_id}` doesn't exist")

        if realm.roles.get(user_id) not in allowed_roles:
            raise BlockAccessError()

        if realm.status.in_maintenance:
            raise BlockInMaintenanceError(f"Realm `{realm_id}` is currently under maintenance")

    async def read(
        self, organization_id: OrganizationID, author: DeviceID, block_id: UUID
    ) -> bytes:
        try:
            blockmeta = self._blockmetas[(organization_id, block_id)]

        except KeyError:
            raise BlockNotFoundError()

        self._check_realm_read_access(organization_id, blockmeta.realm_id, author.user_id)

        return await self._blockstore_component.read(organization_id, block_id)

    async def create(
        self,
        organization_id: OrganizationID,
        author: DeviceID,
        block_id: UUID,
        realm_id: UUID,
        block: bytes,
    ) -> None:
        self._check_realm_write_access(organization_id, realm_id, author.user_id)

        await self._blockstore_component.create(organization_id, block_id, block)
        self._blockmetas[(organization_id, block_id)] = BlockMeta(
            realm_id=realm_id, author=author, size=len(block)
        )


class MemoryBlockStoreComponent(BaseBlockStoreComponent):
    def __init__(self):
        self._blocks = {}

    async def read(self, organization_id: OrganizationID, block_id: UUID) -> bytes:
        try:
            return self._blocks[(organization_id, block_id)]

        except KeyError:
            raise BlockNotFoundError()

    async def create(self, organization_id: OrganizationID, block_id: UUID, block: bytes) -> None:
        key = (organization_id, block_id)
        if key in self._blocks:
            # Should not happen if client play with uuid randomness
            raise BlockAlreadyExistsError()

        self._blocks[key] = block
