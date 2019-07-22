from pypika import Query, Table
from pypika.terms import Function


def fn_exists(q):
    if not q.get_sql():
        q = q.select(True)
    return Function("EXISTS", q)


### Organization ###

t_organization = Table("organization")


def q_organization(organization_id=None, _id=None):
    assert organization_id is not None or _id is not None
    q = Query.from_(t_organization)
    if _id is not None:
        return q.where(t_organization._id == _id)
    else:
        return q.where(t_organization.organization_id == organization_id)


def q_organization_internal_id(organization_id):
    return q_organization(organization_id=organization_id).select("_id")


### User ###

t_user = Table("user_")
t_device = Table("device")
t_user_invitation = Table("user_invitation")
t_device_invitation = Table("device_invitation")


def q_user(organization_id=None, organization=None, user_id=None, _id=None):
    q = Query.from_(t_user).limit(1)
    if _id is not None:
        return q.where(t_user._id == _id)
    else:
        assert user_id is not None
        assert organization_id is not None or organization is not None
        _q_organization = (
            organization
            if organization is not None
            else q_organization_internal_id(organization_id)
        )
        return q.where((t_user.organization == _q_organization) & (t_user.user_id == user_id))


def q_user_internal_id(user_id, organization_id=None, organization=None):
    return q_user(
        organization_id=organization_id, organization=organization, user_id=user_id
    ).select("_id")


def q_device(organization_id=None, organization=None, device_id=None, _id=None):
    q = Query.from_(t_device).limit(1)
    if _id is not None:
        return q.where(t_device._id == _id)
    else:
        assert device_id is not None
        assert organization_id is not None or organization is not None
        _q_organization = (
            organization
            if organization is not None
            else q_organization_internal_id(organization_id)
        )
        return q.where(
            (t_device.organization == _q_organization) & (t_device.device_id == device_id)
        )


def q_device_internal_id(device_id, organization_id=None, organization=None):
    return q_device(
        organization_id=organization_id, organization=organization, device_id=device_id
    ).select("_id")


### Message ###

t_message = Table("message")


### Realm ###

t_realm = Table("realm")
t_realm_user_role = Table("realm_user_role")


def q_realm(organization_id=None, organization=None, realm_id=None, _id=None):
    q = Query.from_(t_realm).limit(1)
    if _id is not None:
        return q.where(t_realm._id == _id)
    else:
        assert realm_id is not None
        assert organization_id is not None or organization is not None
        _q_organization = (
            organization
            if organization is not None
            else q_organization_internal_id(organization_id)
        )
        return q.where((t_realm.organization == _q_organization) & (t_realm.realm_id == realm_id))


def q_realm_internal_id(realm_id, organization_id=None, organization=None):
    return q_realm(
        organization_id=organization_id, organization=organization, realm_id=realm_id
    ).select("_id")


### Vlob ###

t_vlob_encryption_revision = Table("vlob_encryption_revision")
t_vlob_atom = Table("vlob_atom")
t_realm_vlob_update = Table("realm_vlob_update")


def q_user_can_read_vlob(user, realm):
    return fn_exists(
        Query.from_(t_realm_user_role)
        .where((t_realm_user_role.realm == realm) & (t_realm_user_role.user_ == user))
        .limit(1)
    )


def q_user_can_write_vlob(user, realm):
    return fn_exists(
        Query.from_(t_realm_user_role)
        .where(
            (t_realm_user_role.realm == realm)
            & (t_realm_user_role.user_ == user)
            & (t_realm_user_role.role != "READER")
        )
        .limit(1)
    )


### Block ###

t_block = Table("block")
t_block_data = Table("block_data")


def q_block(organization_id=None, organization=None, block_id=None, _id=None):
    q = Query.from_(t_block).limit(1)
    if _id is not None:
        return q.where(t_block._id == _id)
    else:
        assert block_id is not None
        assert organization_id is not None or organization is not None
        _q_organization = (
            organization
            if organization is not None
            else q_organization_internal_id(organization_id)
        )
        return q.where((t_block.organization == _q_organization) & (t_block.block_id == block_id))


def q_block_internal_id(block_id, organization_id=None, organization=None):
    return q_block(
        organization_id=organization_id, organization=organization, block_id=block_id
    ).select("_id")


def q_insert_block(
    block_id,
    size,
    created_on,
    organization=None,
    organization_id=None,
    realm=None,
    realm_id=None,
    author=None,
    author_id=None,
):
    assert organization is not None or organization_id is not None
    assert realm is not None or realm_id is not None
    assert author is not None or author_id is not None

    _q_organization = (
        organization if organization is not None else q_organization_internal_id(organization_id)
    )
    _q_realm = (
        realm
        if realm is not None
        else q_realm_internal_id(organization=_q_organization, realm_id=realm_id)
    )
    _q_author = (
        author
        if author is not None
        else q_device_internal_id(organization=_q_organization, device_id=author_id)
    )

    return (
        Query.into(t_block)
        .columns("organization", "block_id", "realm", "author", "size", "created_on")
        .insert(_q_organization, block_id, _q_realm, _q_author, size, created_on)
    )
