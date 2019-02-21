import pytest
from parsec.core.types import FsPath


def extract_blocks(fs, path):
    access, manifest = fs._local_folder_fs.get_entry(FsPath(path))
    return manifest.dirty_blocks, manifest.blocks


@pytest.mark.trio
async def test_move_between_workspaces(alice_fs):
    await alice_fs.workspace_create("/foo")
    await alice_fs.workspace_create("/bar")
    await alice_fs.touch("/foo/a")
    await alice_fs.file_write("/foo/a", b"hello")
    (block,), () = extract_blocks(alice_fs, "/foo/a")
    await alice_fs.move("/foo/a", "/bar/a")
    assert await alice_fs.file_read("/bar/a") == b"hello"
    (new_block,), () = extract_blocks(alice_fs, "/bar/a")
    assert block.id != new_block.id


@pytest.mark.trio
async def test_move_between_workspaces_after_sync(alice, bob, alice_fs, bob_fs, running_backend):
    # Initialize
    await alice_fs.workspace_create("/foo")
    await alice_fs.workspace_create("/bar")
    await alice_fs.touch("/foo/a")
    await alice_fs.file_write("/foo/a", b"hello")
    await alice_fs.sync("/")

    # Share
    await alice_fs.share("/foo", bob.user_id)
    await alice_fs.share("/bar", bob.user_id)

    # Synchronize
    await bob_fs.process_last_messages()
    assert await alice_fs.file_read("/foo/a") == b"hello"
    assert await bob_fs.file_read("/foo/a") == b"hello"

    # Save blocks
    (), (alice_block,) = extract_blocks(alice_fs, "/foo/a")
    (), (bob_block,) = extract_blocks(bob_fs, "/foo/a")
    assert alice_block.id == bob_block.id

    # Move
    await alice_fs.move("/foo/a", "/bar/a")

    # Synchronize
    await alice_fs.sync("/")
    await bob_fs.sync("/")
    assert await alice_fs.file_read("/bar/a") == b"hello"
    assert await bob_fs.file_read("/bar/a") == b"hello"

    # Compare blocks
    (), (new_alice_block,) = extract_blocks(alice_fs, "/bar/a")
    (), (new_bob_block,) = extract_blocks(bob_fs, "/bar/a")
    assert new_alice_block.id == new_bob_block.id
    assert alice_block.id != new_alice_block.id
    assert bob_block.id != new_bob_block.id
