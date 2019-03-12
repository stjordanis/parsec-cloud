# Parsec Cloud (https://parsec.cloud) Copyright (c) AGPLv3 2019 Scille SAS

from typing import Tuple, Optional
import pendulum
from pendulum import Pendulum

from parsec.types import DeviceID, UserID
from parsec.serde import SerdeError, Serializer, UnknownCheckedSchema, fields

from parsec.crypto import (
    CryptoError,
    VerifyKey,
    SigningKey,
    PublicKey,
    sign_and_add_meta,
    verify_signature_from,
    unsecure_extract_msg_from_signed,
    decode_signedmeta,
)


class TrustChainError(Exception):
    pass


class TrustChainInvalidDataError(TrustChainError):
    pass


class TrustChainTooOldError(TrustChainError):
    pass


class TrustChainCertifServerMismatchError(TrustChainError):
    pass


class TrustChainSignedByRevokedDeviceError(TrustChainError):
    pass


class TrustChainBrokenChainError(TrustChainError):
    pass


# TODO: configurable ?
MAX_TS_BALLPARK = 30 * 60


def timestamps_in_the_ballpark(ts1: Pendulum, ts2: Pendulum) -> bool:
    """
    Useful to compare timestamp provided inside the certified payload and
    the one generated by the backend when it received the certified payload.
    """
    return abs((ts1 - ts2).total_seconds()) < MAX_TS_BALLPARK


class CertifiedDeviceSchema(UnknownCheckedSchema):
    type = fields.CheckedConstant("device", required=True)
    timestamp = fields.DateTime(required=True)
    device_id = fields.DeviceID(required=True)
    verify_key = fields.VerifyKey(required=True)


class CertifiedUserSchema(UnknownCheckedSchema):
    type = fields.CheckedConstant("user", required=True)
    timestamp = fields.DateTime(required=True)
    user_id = fields.UserID(required=True)
    public_key = fields.PublicKey(required=True)


class CertifiedDeviceRevocationSchema(UnknownCheckedSchema):
    type = fields.CheckedConstant("device_revocation", required=True)
    timestamp = fields.DateTime(required=True)
    device_id = fields.DeviceID(required=True)


certified_device_schema = Serializer(CertifiedDeviceSchema)
certified_user_schema = Serializer(CertifiedUserSchema)
certified_device_revocation_schema = Serializer(CertifiedDeviceRevocationSchema)


def _validate_certified_payload(
    schema: UnknownCheckedSchema,
    certifier_key: VerifyKey,
    payload: bytes,
    created_on: Pendulum = None,
) -> dict:
    """
    Raises:
        TrustChainInvalidDataError
        TrustChainTooOldError
    """
    try:
        raw = verify_signature_from(certifier_key, payload)
        data = schema.loads(raw)

    except (CryptoError, SerdeError) as exc:
        raise TrustChainInvalidDataError(*exc.args) from exc

    if not timestamps_in_the_ballpark(data["timestamp"], created_on):
        raise TrustChainTooOldError("Timestamp is too old.")

    return data


def certify_device(
    certifier_id: Optional[DeviceID],
    certifier_key: SigningKey,
    device_id: DeviceID,
    verify_key: VerifyKey,
    now: Pendulum = None,
) -> bytes:
    """
    Raises:
        TrustChainInvalidDataError
    """
    try:
        payload = certified_device_schema.dumps(
            {
                "type": "device",
                "timestamp": now or pendulum.now(),
                "device_id": device_id,
                "verify_key": verify_key,
            }
        )
        return sign_and_add_meta(certifier_id, certifier_key, payload)

    except (CryptoError, SerdeError) as exc:
        raise TrustChainInvalidDataError(*exc.args) from exc


def validate_payload_certified_device(
    certifier_key: VerifyKey, payload: bytes, created_on: Pendulum
) -> dict:
    """
    Raises:
        TrustChainInvalidDataError
        TrustChainTooOldError
    """
    return _validate_certified_payload(certified_device_schema, certifier_key, payload, created_on)


def unsecure_certified_device_extract_verify_key(data: bytes) -> VerifyKey:
    """
    Raises:
        TrustChainInvalidDataError
    """
    try:
        _, signed = decode_signedmeta(data)
        raw = unsecure_extract_msg_from_signed(signed)
        return certified_device_schema.loads(raw)["verify_key"]

    except (CryptoError, SerdeError) as exc:
        raise TrustChainInvalidDataError(*exc.args) from exc


def certify_user(
    certifier_id: Optional[DeviceID],
    certifier_key: SigningKey,
    user_id: UserID,
    public_key: PublicKey,
    now: Pendulum = None,
) -> bytes:
    """
    Raises:
        TrustChainInvalidDataError
    """
    try:
        payload = certified_user_schema.dumps(
            {
                "type": "user",
                "timestamp": now or pendulum.now(),
                "user_id": user_id,
                "public_key": public_key,
            }
        )
        return sign_and_add_meta(certifier_id, certifier_key, payload)

    except (CryptoError, SerdeError) as exc:
        raise TrustChainInvalidDataError(*exc.args) from exc


def validate_payload_certified_user(
    certifier_key: VerifyKey, payload: bytes, created_on: Pendulum
) -> dict:
    """
    Raises:
        TrustChainInvalidDataError
        TrustChainTooOldError
    """
    return _validate_certified_payload(certified_user_schema, certifier_key, payload, created_on)


def unsecure_certified_user_extract_public_key(data: bytes) -> PublicKey:
    """
    Raises:
        TrustChainInvalidDataError
    """
    try:
        _, signed = decode_signedmeta(data)
        raw = unsecure_extract_msg_from_signed(signed)
        return certified_user_schema.loads(raw)["public_key"]

    except (CryptoError, SerdeError) as exc:
        raise TrustChainInvalidDataError(*exc.args) from exc


def certify_device_revocation(
    certifier_id: DeviceID,
    certifier_key: SigningKey,
    revoked_device_id: DeviceID,
    now: Pendulum = None,
) -> bytes:
    """
    Raises:
        TrustChainInvalidDataError
    """
    try:
        payload = certified_device_revocation_schema.dumps(
            {
                "type": "device_revocation",
                "timestamp": now or pendulum.now(),
                "device_id": revoked_device_id,
            }
        )
        return sign_and_add_meta(certifier_id, certifier_key, payload)

    except (CryptoError, SerdeError) as exc:
        raise TrustChainInvalidDataError(*exc.args) from exc


def validate_payload_certified_device_revocation(
    certifier_key: VerifyKey, payload: bytes, revoked_on: Pendulum
) -> dict:
    """
    Raises:
        TrustChainInvalidDataError
        TrustChainTooOldError
    """
    return _validate_certified_payload(
        certified_device_revocation_schema, certifier_key, payload, revoked_on
    )


def certified_extract_parts(certified: bytes) -> Tuple[DeviceID, bytes]:
    """
    Raises:
        TrustChainInvalidDataError
    Returns: Tuple of certifier device id and payload
    """
    try:
        return decode_signedmeta(certified)

    except CryptoError as exc:
        raise TrustChainInvalidDataError(*exc.args) from exc


def validate_user_with_trustchain(user, trustchain, root_verify_key: VerifyKey):
    """
    Returns: Tuple of RemoteDevice that have been validated during the validation of the RemoteUser
    Raises:
        TrustChainBrokenChainError
        TrustChainInvalidDataError
        TrustChainTooOldError
    """
    all_devices = {**trustchain, **{d.device_id: d for d in user.devices.values()}}
    validated_devices = {}

    def _extract_certif_key_and_payload(certified, expected_certifier_id, timestamp, needed_by):
        certifier_id, certified_payload = certified_extract_parts(certified)
        if certifier_id != expected_certifier_id:
            raise TrustChainCertifServerMismatchError(
                f"Device `{needed_by}` is said to be signed by "
                f"`{certifier_id if certifier_id else 'root key'}`"
                f" according to certified payload but by"
                f" `{expected_certifier_id}` according to server"
            )

        if certifier_id:
            try:
                certifier_device = all_devices[certifier_id]

            except KeyError:
                raise TrustChainBrokenChainError(
                    f"Missing `{certifier_id}` needed to validate `{needed_by}`"
                )

            _recursive_validate_device(certifier_device)
            if certifier_device.revoked_on and timestamp > certifier_device.revoked_on:
                raise TrustChainSignedByRevokedDeviceError(
                    f"Device `{certifier_id}` signed `{needed_by}` after it revocation "
                    f"(revoked at {certifier_device.revoked_on}, signed at {timestamp})"
                )
            certifier_verify_key = certifier_device.verify_key

        else:
            certifier_verify_key = root_verify_key

        return certifier_verify_key, certified_payload

    def _recursive_validate_device(device):
        if device.device_id in validated_devices:
            return

        certifier_verify_key, certified_payload = _extract_certif_key_and_payload(
            device.certified_device, device.device_certifier, device.created_on, device.device_id
        )
        validate_payload_certified_device(
            certifier_verify_key, certified_payload, device.created_on
        )

        if device.certified_revocation:
            certifier_verify_key, certified_payload = _extract_certif_key_and_payload(
                device.certified_revocation,
                device.revocation_certifier,
                device.revoked_on,
                device.device_id,
            )
            validate_payload_certified_device_revocation(
                certifier_verify_key, certified_payload, device.revoked_on
            )
        # All set ! This device is valid ;-)
        validated_devices[device.device_id] = device

    # Validate the user first
    certifier_verify_key, certified_payload = _extract_certif_key_and_payload(
        user.certified_user, user.user_certifier, user.created_on, user.user_id
    )
    validate_payload_certified_user(certifier_verify_key, certified_payload, user.created_on)
    # Now validate the devices
    for device in user.devices.values():
        _recursive_validate_device(device)

    return validated_devices
