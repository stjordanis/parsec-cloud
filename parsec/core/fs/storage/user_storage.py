# Parsec Cloud (https://parsec.cloud) Copyright (c) AGPLv3 2019 Scille SAS

from pathlib import Path
from typing import Dict, Set, Tuple
from async_generator import asynccontextmanager

from parsec.core.fs.exceptions import FSLocalMissError
from parsec.core.types import EntryID, LocalDevice, LocalUserManifest

from parsec.core.fs.storage.version import USER_STORAGE_NAME
from parsec.core.fs.storage.manifest_storage import ManifestStorage


class UserStorage:
    """Storage for the user manifest.

    Provides a synchronous interface to the user manifest as it is used very often.
    """

    def __init__(self, device, path, user_manifest_id, manifest_storage):
        self.path = path
        self.device = device
        self.user_manifest_id = user_manifest_id
        self.manifest_storage = manifest_storage

    @classmethod
    @asynccontextmanager
    async def run(cls, device: LocalDevice, path: Path, user_manifest_id: EntryID):
        manifest_storage_context = ManifestStorage.run(
            device, path / USER_STORAGE_NAME, user_manifest_id
        )
        async with manifest_storage_context as manifest_storage:
            self = cls(device, path, user_manifest_id, manifest_storage)

            # Load the user manifest
            try:
                await self.load_user_manifest()
            except FSLocalMissError:
                pass
            else:
                assert self.user_manifest_id in self.manifest_storage._cache

            yield self

    # Checkpoint interface

    async def get_realm_checkpoint(self) -> int:
        return await self.manifest_storage.get_realm_checkpoint()

    async def update_realm_checkpoint(
        self, new_checkpoint: int, changed_vlobs: Dict[EntryID, int]
    ) -> None:
        """
        Raises: Nothing !
        """
        await self.manifest_storage.update_realm_checkpoint(new_checkpoint, changed_vlobs)

    async def get_need_sync_entries(self) -> Tuple[Set[EntryID], Set[EntryID]]:
        return await self.manifest_storage.get_need_sync_entries()

    # User manifest

    def get_user_manifest(self):
        try:
            return self.manifest_storage._cache[self.user_manifest_id]
        except KeyError:
            # In the unlikely event the user manifest is not present in
            # local (e.g. device just created or during tests), we fall
            # back on an empty manifest which is a good aproximation of
            # the very first version of the manifest (field `created` is
            # invalid, but it will be corrected by the merge during sync).
            return LocalUserManifest.new_placeholder(id=self.device.user_manifest_id)

    async def load_user_manifest(self) -> LocalUserManifest:
        return await self.manifest_storage.get_manifest(self.user_manifest_id)

    async def set_user_manifest(self, user_manifest: LocalUserManifest):
        assert self.user_manifest_id == user_manifest.id
        await self.manifest_storage.set_manifest(self.user_manifest_id, user_manifest)

    # No vacuuming (used in sync monitor)

    async def run_vacuum(self) -> None:
        pass
