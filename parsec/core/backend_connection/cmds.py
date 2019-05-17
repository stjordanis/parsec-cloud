# Parsec Cloud (https://parsec.cloud) Copyright (c) AGPLv3 2019 Scille SAS

from typing import Tuple, List, Dict, Iterable, Optional
from uuid import UUID
import pendulum

from parsec.types import DeviceID, UserID, DeviceName, OrganizationID
from parsec.crypto import VerifyKey
from parsec.api.transport import Transport, TransportError
from parsec.api.protocole import (
    ProtocoleError,
    ping_serializer,
    organization_create_serializer,
    organization_bootstrap_serializer,
    events_subscribe_serializer,
    events_listen_serializer,
    message_send_serializer,
    message_get_serializer,
    VlobGroupRole,
    vlob_group_check_serializer,
    vlob_read_serializer,
    vlob_create_serializer,
    vlob_update_serializer,
    vlob_group_update_roles_serializer,
    vlob_group_get_roles_serializer,
    vlob_group_poll_serializer,
    block_create_serializer,
    block_read_serializer,
    user_get_serializer,
    user_find_serializer,
    user_invite_serializer,
    user_get_invitation_creator_serializer,
    user_claim_serializer,
    user_cancel_invitation_serializer,
    user_create_serializer,
    device_invite_serializer,
    device_get_invitation_creator_serializer,
    device_claim_serializer,
    device_cancel_invitation_serializer,
    device_create_serializer,
    device_revoke_serializer,
)
from parsec.core.types import UnverifiedRemoteUser, UnverifiedRemoteDevice
from parsec.core.backend_connection.exceptions import (
    raise_on_bad_response,
    BackendNotAvailable,
    BackendCmdsInvalidRequest,
    BackendCmdsInvalidResponse,
)


async def _send_cmd(transport, serializer, keepalive=False, **req):
    """
    Raises:
        BackendCmdsInvalidRequest
        BackendCmdsInvalidResponse
        BackendNotAvailable
        BackendCmdsBadResponse
    """
    transport.logger.info("Request", cmd=req["cmd"])

    def _shorten_data(data):
        if len(req) > 300:
            return data[:150] + b"[...]" + data[-150:]
        else:
            return data

    try:
        raw_req = serializer.req_dumps(req)

    except ProtocoleError as exc:
        raise BackendCmdsInvalidRequest(exc) from exc

    try:
        await transport.send(raw_req)
        raw_rep = await transport.recv(keepalive)

    except TransportError as exc:
        transport.logger.info("Request failed (backend not available)", cmd=req["cmd"])
        raise BackendNotAvailable(exc) from exc

    try:
        rep = serializer.rep_loads(raw_rep)

    except ProtocoleError as exc:
        transport.logger.warning("Request failed (bad protocol)", cmd=req["cmd"], error=exc)
        raise BackendCmdsInvalidResponse(exc) from exc

    if rep["status"] == "invalid_msg_format":
        raise BackendCmdsInvalidRequest(rep)

    raise_on_bad_response(rep)

    return rep


###  Backend authenticated cmds  ###


### Events&misc API ###


async def ping(transport: Transport, ping: str) -> str:
    rep = await _send_cmd(transport, ping_serializer, cmd="ping", ping=ping)
    return rep["pong"]


async def events_subscribe(
    transport: Transport,
    message_received: bool = False,
    vlob_group_updated: Iterable[UUID] = (),
    pinged: Iterable[str] = (),
) -> None:
    await _send_cmd(
        transport,
        events_subscribe_serializer,
        cmd="events_subscribe",
        message_received=message_received,
        vlob_group_updated=vlob_group_updated,
        pinged=pinged,
    )


async def events_listen(transport: Transport, wait: bool = True) -> dict:
    rep = await _send_cmd(
        transport, events_listen_serializer, keepalive=wait, cmd="events_listen", wait=wait
    )
    rep.pop("status")
    return rep


### Message API ###


async def message_send(
    transport: Transport, recipient: UserID, timestamp: pendulum.Pendulum, body: bytes
) -> None:
    await _send_cmd(
        transport,
        message_send_serializer,
        cmd="message_send",
        recipient=recipient,
        timestamp=timestamp,
        body=body,
    )


async def message_get(transport: Transport, offset: int) -> List[Tuple[int, DeviceID, bytes]]:
    rep = await _send_cmd(transport, message_get_serializer, cmd="message_get", offset=offset)
    return [
        (item["count"], item["sender"], item["timestamp"], item["body"]) for item in rep["messages"]
    ]


### Vlob API ###


async def vlob_group_check(transport: Transport, to_check: list) -> list:
    rep = await _send_cmd(
        transport, vlob_group_check_serializer, cmd="vlob_group_check", to_check=to_check
    )
    return rep["changed"]


async def vlob_create(
    transport: Transport, group: UUID, id: UUID, timestamp: pendulum.Pendulum, blob: bytes
) -> None:
    await _send_cmd(
        transport,
        vlob_create_serializer,
        cmd="vlob_create",
        group=group,
        id=id,
        timestamp=timestamp,
        blob=blob,
    )


async def vlob_read(
    transport: Transport, id: UUID, version: int = None
) -> Tuple[DeviceID, pendulum.Pendulum, int, bytes]:
    rep = await _send_cmd(transport, vlob_read_serializer, cmd="vlob_read", id=id, version=version)
    return rep["author"], rep["timestamp"], rep["version"], rep["blob"]


async def vlob_update(
    transport: Transport, id: UUID, version: int, timestamp: pendulum.Pendulum, blob: bytes
) -> None:
    await _send_cmd(
        transport,
        vlob_update_serializer,
        cmd="vlob_update",
        id=id,
        version=version,
        timestamp=timestamp,
        blob=blob,
    )


async def vlob_group_get_roles(transport: Transport, id: UUID) -> Dict[UserID, VlobGroupRole]:
    rep = await _send_cmd(
        transport, vlob_group_get_roles_serializer, cmd="vlob_group_get_roles", id=id
    )
    return rep["users"]


async def vlob_group_update_roles(
    transport: Transport, id: UUID, user: UserID, role: Optional[VlobGroupRole]
) -> None:
    await _send_cmd(
        transport,
        vlob_group_update_roles_serializer,
        cmd="vlob_group_update_roles",
        id=id,
        user=user,
        role=role,
    )


async def vlob_group_poll(
    transport: Transport, id: UUID, last_checkpoint: int
) -> Tuple[int, Dict[UUID, int]]:
    rep = await _send_cmd(
        transport,
        vlob_group_poll_serializer,
        cmd="vlob_group_poll",
        id=id,
        last_checkpoint=last_checkpoint,
    )
    return (rep["current_checkpoint"], rep["changes"])


### Block API ###


async def block_create(transport: Transport, id: UUID, vlob_group: UUID, block: bytes) -> None:
    await _send_cmd(
        transport,
        block_create_serializer,
        cmd="block_create",
        id=id,
        vlob_group=vlob_group,
        block=block,
    )


async def block_read(transport: Transport, id: UUID) -> bytes:
    rep = await _send_cmd(transport, block_read_serializer, cmd="block_read", id=id)
    return rep["block"]


### User API ###


async def user_get(
    transport: Transport, user_id: UserID
) -> Tuple[UnverifiedRemoteUser, List[UnverifiedRemoteDevice], List[UnverifiedRemoteDevice]]:
    rep = await _send_cmd(transport, user_get_serializer, cmd="user_get", user_id=user_id)

    print("user_get")
    print(dict(rep))
    user = UnverifiedRemoteUser(user_certificate=rep["user_certificate"])
    import pdb

    pdb.set_trace()
    devices = [
        UnverifiedRemoteDevice(
            device_certificate=d["device_certificate"],
            revoked_device_certificate=d.get("revoked_device_certificate"),
        )
        for d in rep["devices"]
    ]
    trustchain = [
        UnverifiedRemoteDevice(
            device_certificate=d["device_certificate"],
            revoked_device_certificate=d.get("revoked_device_certificate"),
        )
        for d in rep["trustchain"]
    ]
    return (user, devices, trustchain)


async def user_find(
    transport: Transport,
    query: str = None,
    page: int = 1,
    per_page: int = 100,
    omit_revoked: bool = False,
) -> List[UserID]:
    rep = await _send_cmd(
        transport,
        user_find_serializer,
        cmd="user_find",
        query=query,
        page=page,
        per_page=per_page,
        omit_revoked=omit_revoked,
    )
    return rep["results"]


async def user_invite(transport: Transport, user_id: UserID) -> bytes:
    rep = await _send_cmd(transport, user_invite_serializer, cmd="user_invite", user_id=user_id)
    return rep["encrypted_claim"]


async def user_cancel_invitation(transport: Transport, user_id: UserID) -> None:
    await _send_cmd(
        transport, user_cancel_invitation_serializer, cmd="user_cancel_invitation", user_id=user_id
    )


async def user_create(
    transport: Transport, user_certificate: bytes, device_certificate: bytes
) -> None:
    await _send_cmd(
        transport,
        user_create_serializer,
        cmd="user_create",
        user_certificate=user_certificate,
        device_certificate=device_certificate,
    )


async def device_invite(transport: Transport, invited_device_name: DeviceName) -> bytes:
    rep = await _send_cmd(
        transport,
        device_invite_serializer,
        cmd="device_invite",
        invited_device_name=invited_device_name,
    )
    return rep["encrypted_claim"]


async def device_cancel_invitation(transport: Transport, invited_device_name: DeviceName) -> None:
    await _send_cmd(
        transport,
        device_cancel_invitation_serializer,
        cmd="device_cancel_invitation",
        invited_device_name=invited_device_name,
    )


async def device_create(
    transport: Transport, device_certificate: bytes, encrypted_answer: bytes
) -> None:
    await _send_cmd(
        transport,
        device_create_serializer,
        cmd="device_create",
        device_certificate=device_certificate,
        encrypted_answer=encrypted_answer,
    )


async def device_revoke(
    transport: Transport, revoked_device_certificate: bytes
) -> Optional[pendulum.Pendulum]:
    rep = await _send_cmd(
        transport,
        device_revoke_serializer,
        cmd="device_revoke",
        revoked_device_certificate=revoked_device_certificate,
    )
    return rep["user_revoked_on"]


###  Backend anonymous cmds  ###


# ping already defined in authenticated part


async def organization_create(transport: Transport, organization_id: OrganizationID) -> str:
    rep = await _send_cmd(
        transport,
        organization_create_serializer,
        cmd="organization_create",
        organization_id=organization_id,
    )
    return rep["bootstrap_token"]


async def organization_bootstrap(
    transport: Transport,
    organization_id: OrganizationID,
    bootstrap_token: str,
    root_verify_key: VerifyKey,
    user_certificate: bytes,
    device_certificate: bytes,
) -> None:
    await _send_cmd(
        transport,
        organization_bootstrap_serializer,
        cmd="organization_bootstrap",
        organization_id=organization_id,
        bootstrap_token=bootstrap_token,
        root_verify_key=root_verify_key,
        user_certificate=user_certificate,
        device_certificate=device_certificate,
    )


async def user_get_invitation_creator(
    transport: Transport, invited_user_id: UserID
) -> Tuple[UnverifiedRemoteUser, List[UnverifiedRemoteDevice]]:
    rep = await _send_cmd(
        transport,
        user_get_invitation_creator_serializer,
        cmd="user_get_invitation_creator",
        invited_user_id=invited_user_id,
    )

    print("user_get_invitation_creator")
    print(dict(rep))

    user = UnverifiedRemoteUser(user_certificate=rep["user_certificate"])

    trustchain = [
        UnverifiedRemoteDevice(
            device_certificate=d["device_certificate"],
            revoked_device_certificate=d.get("revoked_device_certificate"),
        )
        for d in rep["trustchain"]
    ]
    return (user, trustchain)


async def user_claim(transport: Transport, invited_user_id: UserID, encrypted_claim: bytes) -> None:
    await _send_cmd(
        transport,
        user_claim_serializer,
        cmd="user_claim",
        invited_user_id=invited_user_id,
        encrypted_claim=encrypted_claim,
    )


async def device_get_invitation_creator(
    transport: Transport, invited_device_id: DeviceID
) -> Tuple[UnverifiedRemoteUser, List[UnverifiedRemoteDevice]]:
    rep = await _send_cmd(
        transport,
        device_get_invitation_creator_serializer,
        cmd="device_get_invitation_creator",
        invited_device_id=invited_device_id,
    )

    print("device_get_invitation_creator")
    print(dict(rep))
    user = UnverifiedRemoteUser(user_certificate=rep["user_certificate"])

    trustchain = [
        UnverifiedRemoteDevice(
            device_certificate=d["device_certificate"],
            revoked_device_certificate=d.get("revoked_device_certificate"),
        )
        for d in rep["trustchain"]
    ]
    return (user, trustchain)


async def device_claim(
    transport: Transport, invited_device_id: DeviceID, encrypted_claim: bytes
) -> bytes:
    rep = await _send_cmd(
        transport,
        device_claim_serializer,
        cmd="device_claim",
        invited_device_id=invited_device_id,
        encrypted_claim=encrypted_claim,
    )
    return rep["encrypted_answer"]
