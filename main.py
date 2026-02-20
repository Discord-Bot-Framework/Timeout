from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import datetime
import enum
import math
import pathlib
import typing
from collections import defaultdict

import arc
import hikari
import miru
from miru.ext import nav
from src.container.app import get_miru
from src.shared.logger import get_module_logger
from src.shared.persistence.repository import (
    delete_record,
    list_mapping_records,
    put_mapping_record,
)
from src.shared.persistence.store import Store
from src.shared.utils.view import (
    Color,
    bind_view_to_response,
    defer,
    reply_embed,
    reply_err,
    reply_ok,
    respond_with_builder_and_bind_view,
)

if typing.TYPE_CHECKING:
    from collections.abc import (
        Awaitable,
        Callable,
        Coroutine,
        Generator,
        Mapping,
        Sequence,
    )

    import lmdb

DB_PATH: pathlib.Path = pathlib.Path(__file__).parent / "timeout"
DB_MAP_SIZE: int = 50 * 1024 * 1024
DB_ADMIN: typing.Literal["admin"] = "admin"
DB_CHANNEL_MODERATOR: typing.Literal["channel_moderator"] = "channel_moderator"
DB_GUILD_MODERATOR: typing.Literal["guild_moderator"] = "guild_moderator"
DB_TIMEOUT_MEMBER: typing.Literal["timeout_member"] = "timeout_member"
DB_SETTING: typing.Literal["primary_value"] = "primary_value"
DBS: tuple[str, ...] = (
    DB_ADMIN,
    DB_CHANNEL_MODERATOR,
    DB_GUILD_MODERATOR,
    DB_TIMEOUT_MEMBER,
    DB_SETTING,
)
logger = get_module_logger(__file__, __name__, "timeout.log")


database = Store(DB_PATH, DBS, map_size=DB_MAP_SIZE)


class BaseDB:
    __slots__ = ()

    @classmethod
    def get_database(cls) -> str:
        raise NotImplementedError

    @classmethod
    def get_databases(cls, env: lmdb.Environment | None) -> list[dict[str, typing.Any]]:
        return list_mapping_records(env, database, cls.get_database())

    @classmethod
    def delete(cls, env: lmdb.Environment | None, key: bytes) -> bool:
        return delete_record(env, database, cls.get_database(), key)


class AdminDB(BaseDB):
    __slots__ = ()

    @classmethod
    def get_database(cls) -> typing.Literal["admin"]:
        return DB_ADMIN

    @classmethod
    def get_databases(cls, env: lmdb.Environment | None) -> list[dict[str, int]]:
        return typing.cast("list[dict[str, int]]", super().get_databases(env))

    @classmethod
    def add(cls, env: lmdb.Environment | None, id: int, type: int) -> None:
        key: bytes = f"{id}:{type}".encode()
        put_mapping_record(
            env,
            database,
            cls.get_database(),
            key,
            {"id": id, "type": type},
        )

    @classmethod
    def delete_admin(cls, env: lmdb.Environment | None, id: int, type: int) -> bool:
        key: bytes = f"{id}:{type}".encode()
        return BaseDB.delete(env, key)


class ModeratorDB(BaseDB):
    __slots__ = ()

    @classmethod
    def get_database(cls) -> typing.Literal["channel_moderator"]:
        return DB_CHANNEL_MODERATOR

    @classmethod
    def get_databases(cls, env: lmdb.Environment | None) -> list[dict[str, int]]:
        return typing.cast("list[dict[str, int]]", super().get_databases(env))

    @classmethod
    def add(
        cls,
        env: lmdb.Environment | None,
        id: int,
        type: int,
        channel_id: int,
    ) -> None:
        key: bytes = f"{id}:{type}:{channel_id}".encode()
        put_mapping_record(
            env,
            database,
            cls.get_database(),
            key,
            {"id": id, "type": type, "channel_id": channel_id},
        )

    @classmethod
    def delete_moderator(
        cls,
        env: lmdb.Environment | None,
        id: int,
        type: int,
        channel_id: int,
    ) -> bool:
        key: bytes = f"{id}:{type}:{channel_id}".encode()
        return BaseDB.delete(env, key)


class GlobalModeratorDB(BaseDB):
    __slots__ = ()

    @classmethod
    def get_database(cls) -> typing.Literal["guild_moderator"]:
        return "guild_moderator"

    @classmethod
    def get_databases(cls, env: lmdb.Environment | None) -> list[dict[str, int]]:
        return typing.cast("list[dict[str, int]]", super().get_databases(env))

    @classmethod
    def add(cls, env: lmdb.Environment | None, id: int, type: int) -> None:
        key: bytes = f"{id}:{type}".encode()
        put_mapping_record(
            env,
            database,
            cls.get_database(),
            key,
            {"id": id, "type": type},
        )

    @classmethod
    def delete_global_moderator(
        cls,
        env: lmdb.Environment | None,
        id: int,
        type: int,
    ) -> bool:
        key: bytes = f"{id}:{type}".encode()
        return BaseDB.delete(env, key)


class TimeoutMemberDB(BaseDB):
    __slots__ = ()

    @classmethod
    def get_database(cls) -> typing.Literal["timeout_member"]:
        return DB_TIMEOUT_MEMBER

    @classmethod
    def get_databases(cls, env: lmdb.Environment | None) -> list[dict[str, typing.Any]]:
        result = super().get_databases(env)
        for data in result:
            release_dt = data.get("release_datetime")
            if isinstance(release_dt, str):
                try:
                    data["release_datetime"] = datetime.datetime.fromisoformat(
                        release_dt,
                    )
                except ValueError:
                    data["release_datetime"] = datetime.datetime.now(datetime.UTC)
        return result

    @classmethod
    def add(
        cls,
        env: lmdb.Environment | None,
        id: int,
        channel_id: int,
        release_datetime: datetime.datetime,
    ) -> None:
        key: bytes = f"{id}:{channel_id}".encode()
        put_mapping_record(
            env,
            database,
            cls.get_database(),
            key,
            {
                "id": id,
                "channel_id": channel_id,
                "release_datetime": release_datetime.isoformat(),
            },
        )

    @classmethod
    def delete_prisoner(
        cls,
        env: lmdb.Environment | None,
        id: int,
        channel_id: int,
    ) -> bool:
        key: bytes = f"{id}:{channel_id}".encode()
        return BaseDB.delete(env, key)


class SettingDB(BaseDB):
    __slots__ = ()

    @classmethod
    def get_database(cls) -> typing.Literal["primary_value"]:
        return DB_SETTING

    @classmethod
    def get_databases(cls, env: lmdb.Environment | None) -> list[dict[str, typing.Any]]:
        return super().get_databases(env)

    @classmethod
    def upsert_setting(
        cls,
        env: lmdb.Environment | None,
        type: int,
        primary_value: int,
        secondary_value: str | None,
    ) -> None:
        key: bytes = str(type).encode()
        put_mapping_record(
            env,
            database,
            cls.get_database(),
            key,
            {
                "type": type,
                "primary_value": primary_value,
                "secondary_value": secondary_value,
            },
        )


database.open()
env: lmdb.Environment | None = database.env


@enum.unique
class MRCTType(int, enum.Enum):
    USER = 1
    ROLE = 2


@enum.unique
class SettingType(int, enum.Enum):
    LOG_CHANNEL = 0
    MINUTE_LIMIT = 1


@dataclasses.dataclass(frozen=True, slots=True)
class Admin:
    id: int
    type: MRCTType | int


@dataclasses.dataclass(frozen=True, slots=True)
class ChannelModerator:
    id: int
    type: MRCTType | int
    channel_id: int


@dataclasses.dataclass(frozen=True, slots=True)
class GuildModerator:
    id: int
    type: MRCTType | int


@dataclasses.dataclass(frozen=True, slots=True, eq=True, order=True)
class TimeoutMember:
    id: int
    release_datetime: datetime.datetime
    channel_id: int

    @property
    def is_global(self) -> bool:
        return self.channel_id == -1

    def to_tuple(self) -> tuple[int, int]:
        return (self.id, self.channel_id)


@dataclasses.dataclass(frozen=True, slots=True)
class Config:
    type: SettingType | int
    primary_value: int
    secondary_value: str | None

    def upsert(self, setting_list: list[Config]) -> None:
        for i, conf in enumerate(setting_list):
            if self.type == conf.type:
                setting_list[i] = self
                return
        setting_list.append(self)
        setting_list.sort(key=lambda c: c.type)

    @staticmethod
    def sort_list(setting_list: list[Config]) -> None:
        setting_list.sort(key=lambda c: c.type)


admins: list[Admin] = []
channel_moderators: list[ChannelModerator] = []
guild_moderators: list[GuildModerator] = []
timeout_members: list[TimeoutMember] = []
timeout_tasks: dict[tuple[int, int], asyncio.Task[None]] = {}
settings: list[Config] = []

_admin_set: set[Admin] = set()
_admin_user_ids: set[int] = set()
_admin_role_ids: set[int] = set()
_channel_moderator_set: set[ChannelModerator] = set()
_channel_moderator_user_ids: dict[int, set[int]] = defaultdict(set)
_channel_moderator_role_ids: dict[int, set[int]] = defaultdict(set)
_guild_moderator_set: set[GuildModerator] = set()
_guild_moderator_user_ids: set[int] = set()
_guild_moderator_role_ids: set[int] = set()
_timeout_member_by_key: dict[tuple[int, int], TimeoutMember] = {}
_timeout_members_by_user: dict[int, set[tuple[int, int]]] = defaultdict(set)
_global_timeout_user_ids: set[int] = set()
_settings_by_type: dict[SettingType, Config] = {}


def _now_utc() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)


def _ensure_utc(dt: datetime.datetime) -> datetime.datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=datetime.UTC)
    return dt.astimezone(datetime.UTC)


def _rebuild_admin_index() -> None:
    _admin_set.clear()
    _admin_user_ids.clear()
    _admin_role_ids.clear()
    _admin_set.update(admins)
    for admin in admins:
        if int(admin.type) == int(MRCTType.USER):
            _admin_user_ids.add(admin.id)
        elif int(admin.type) == int(MRCTType.ROLE):
            _admin_role_ids.add(admin.id)


def _rebuild_channel_moderator_index() -> None:
    _channel_moderator_set.clear()
    _channel_moderator_user_ids.clear()
    _channel_moderator_role_ids.clear()
    _channel_moderator_set.update(channel_moderators)
    for moderator in channel_moderators:
        if int(moderator.type) == int(MRCTType.USER):
            _channel_moderator_user_ids[moderator.channel_id].add(moderator.id)
        elif int(moderator.type) == int(MRCTType.ROLE):
            _channel_moderator_role_ids[moderator.channel_id].add(moderator.id)


def _rebuild_guild_moderator_index() -> None:
    _guild_moderator_set.clear()
    _guild_moderator_user_ids.clear()
    _guild_moderator_role_ids.clear()
    _guild_moderator_set.update(guild_moderators)
    for moderator in guild_moderators:
        if int(moderator.type) == int(MRCTType.USER):
            _guild_moderator_user_ids.add(moderator.id)
        elif int(moderator.type) == int(MRCTType.ROLE):
            _guild_moderator_role_ids.add(moderator.id)


def _rebuild_timeout_index() -> None:
    _timeout_member_by_key.clear()
    _timeout_members_by_user.clear()
    _global_timeout_user_ids.clear()
    for timeout_member in timeout_members:
        key = timeout_member.to_tuple()
        _timeout_member_by_key[key] = timeout_member
        _timeout_members_by_user[timeout_member.id].add(key)
        if timeout_member.is_global:
            _global_timeout_user_ids.add(timeout_member.id)


def _rebuild_settings_index() -> None:
    settings.sort(key=lambda cfg: int(cfg.type))
    _settings_by_type.clear()
    for cfg in settings:
        if isinstance(cfg.type, SettingType):
            _settings_by_type[cfg.type] = cfg
        else:
            with contextlib.suppress(ValueError):
                _settings_by_type[SettingType(int(cfg.type))] = cfg


def _upsert_setting_cache(config: Config) -> None:
    for idx, existing in enumerate(settings):
        if int(existing.type) == int(config.type):
            settings[idx] = config
            break
    else:
        settings.append(config)
    _rebuild_settings_index()


def _setting(setting_type: SettingType) -> Config:
    config = _settings_by_type.get(setting_type)
    if config is not None:
        return config
    fallback = Config(setting_type, 600, None)
    _upsert_setting_cache(fallback)
    return fallback


def _add_admin(admin: Admin) -> bool:
    if admin in _admin_set:
        return False
    admins.append(admin)
    _admin_set.add(admin)
    if int(admin.type) == int(MRCTType.USER):
        _admin_user_ids.add(admin.id)
    else:
        _admin_role_ids.add(admin.id)
    return True


def _remove_admin(admin: Admin) -> bool:
    if admin not in _admin_set:
        return False
    _admin_set.remove(admin)
    with contextlib.suppress(ValueError):
        admins.remove(admin)
    if int(admin.type) == int(MRCTType.USER):
        _admin_user_ids.discard(admin.id)
    else:
        _admin_role_ids.discard(admin.id)
    return True


def _add_guild_moderator(moderator: GuildModerator) -> bool:
    if moderator in _guild_moderator_set:
        return False
    guild_moderators.append(moderator)
    _guild_moderator_set.add(moderator)
    if int(moderator.type) == int(MRCTType.USER):
        _guild_moderator_user_ids.add(moderator.id)
    else:
        _guild_moderator_role_ids.add(moderator.id)
    return True


def _remove_guild_moderator(moderator: GuildModerator) -> bool:
    if moderator not in _guild_moderator_set:
        return False
    _guild_moderator_set.remove(moderator)
    with contextlib.suppress(ValueError):
        guild_moderators.remove(moderator)
    if int(moderator.type) == int(MRCTType.USER):
        _guild_moderator_user_ids.discard(moderator.id)
    else:
        _guild_moderator_role_ids.discard(moderator.id)
    return True


def _add_channel_moderator(moderator: ChannelModerator) -> bool:
    if moderator in _channel_moderator_set:
        return False
    channel_moderators.append(moderator)
    _channel_moderator_set.add(moderator)
    if int(moderator.type) == int(MRCTType.USER):
        _channel_moderator_user_ids[moderator.channel_id].add(moderator.id)
    else:
        _channel_moderator_role_ids[moderator.channel_id].add(moderator.id)
    return True


def _remove_channel_moderator(moderator: ChannelModerator) -> bool:
    if moderator not in _channel_moderator_set:
        return False
    _channel_moderator_set.remove(moderator)
    with contextlib.suppress(ValueError):
        channel_moderators.remove(moderator)
    if int(moderator.type) == int(MRCTType.USER):
        _channel_moderator_user_ids.get(moderator.channel_id, set()).discard(
            moderator.id,
        )
    else:
        _channel_moderator_role_ids.get(moderator.channel_id, set()).discard(
            moderator.id,
        )
    return True


def _upsert_timeout_member(timeout_member: TimeoutMember) -> None:
    key = timeout_member.to_tuple()
    previous = _timeout_member_by_key.get(key)
    if previous is None:
        timeout_members.append(timeout_member)
    else:
        for idx, existing in enumerate(timeout_members):
            if existing.to_tuple() == key:
                timeout_members[idx] = timeout_member
                break
    _timeout_member_by_key[key] = timeout_member
    _timeout_members_by_user[timeout_member.id].add(key)
    if any(user_key[1] == -1 for user_key in _timeout_members_by_user[timeout_member.id]):
        _global_timeout_user_ids.add(timeout_member.id)


def _delete_timeout_member(timeout_member: TimeoutMember) -> bool:
    key = timeout_member.to_tuple()
    existing = _timeout_member_by_key.pop(key, None)
    if existing is None:
        return False
    with contextlib.suppress(ValueError):
        timeout_members.remove(existing)
    user_keys = _timeout_members_by_user.get(timeout_member.id)
    if user_keys is not None:
        user_keys.discard(key)
        if not user_keys:
            _timeout_members_by_user.pop(timeout_member.id, None)
    if not any(user_key[1] == -1 for user_key in _timeout_members_by_user.get(timeout_member.id, set())):
        _global_timeout_user_ids.discard(timeout_member.id)
    return True


def _get_timeout_member(user_id: int, channel_id: int) -> TimeoutMember | None:
    return _timeout_member_by_key.get((user_id, channel_id))


def _get_timeout_members_for_user(user_id: int) -> list[TimeoutMember]:
    keys = _timeout_members_by_user.get(user_id)
    if not keys:
        return []
    return [member for key in keys if (member := _timeout_member_by_key.get(key)) is not None]


def _has_any_admin_role(
    role_ids: Sequence[hikari.SnowflakeishOr[hikari.PartialRole] | int],
) -> bool:
    return any(int(role_id) in _admin_role_ids for role_id in role_ids)


def _has_any_guild_moderator_role(
    role_ids: Sequence[hikari.SnowflakeishOr[hikari.PartialRole] | int],
) -> bool:
    return any(int(role_id) in _guild_moderator_role_ids for role_id in role_ids)


def _has_any_channel_moderator_role(
    channel_id: int,
    role_ids: Sequence[hikari.SnowflakeishOr[hikari.PartialRole] | int],
) -> bool:
    channel_roles = _channel_moderator_role_ids.get(channel_id)
    if not channel_roles:
        return False
    return any(int(role_id) in channel_roles for role_id in role_ids)


def fetch_parent_id(
    channel: hikari.GuildChannel | hikari.GuildThreadChannel,
) -> int:
    if isinstance(channel, hikari.GuildThreadChannel):
        if channel.parent_id is None:
            msg = "Thread channel has no parent"
            raise RuntimeError(msg)
        return int(channel.parent_id)
    return int(channel.id)


async def fetch_parent_channel(
    state: TimeoutState,
    channel: hikari.GuildChannel | hikari.GuildThreadChannel,
) -> hikari.GuildChannel:
    if isinstance(channel, hikari.GuildThreadChannel):
        if channel.parent_id is None:
            msg = "Thread channel has no parent"
            raise RuntimeError(msg)
        return await fetch_channel(state, channel.parent_id)
    return channel


def mention_channel(channel_id: int) -> str:
    return f"<#{channel_id}>"


def message_flags(
    *,
    ephemeral: bool = False,
    silent: bool = False,
) -> hikari.MessageFlag:
    flags = hikari.MessageFlag.NONE
    if ephemeral:
        flags |= hikari.MessageFlag.EPHEMERAL
    if silent:
        flags |= hikari.MessageFlag.SUPPRESS_NOTIFICATIONS
    return flags


def is_bot(user: hikari.User | hikari.Member) -> bool:
    return bool(getattr(user, "is_bot", False))


def name_display(user: hikari.User | hikari.Member) -> str:
    return getattr(user, "name_display", user.username)


def user_display(user: hikari.User | hikari.Member | None, user_id: int | None) -> str:
    if user_id is None:
        return ""
    if user is None:
        return f"<@{user_id}>"
    return user.mention


def role_display(role: hikari.Role | None, role_id: int | None) -> str:
    if role_id is None:
        return ""
    if role is None:
        return f"<@&{role_id}>"
    return role.mention


async def fetch_member(
    state: TimeoutState,
    guild_id: int,
    user_id: int,
) -> hikari.Member | None:
    cache = state.client.app.cache
    if cache is not None:
        cached = cache.get_member(guild_id, user_id)
        if cached is not None:
            return cached
    try:
        return await state.client.app.rest.fetch_member(guild_id, user_id)
    except hikari.NotFoundError:
        return None


async def fetch_role(
    state: TimeoutState,
    guild_id: int,
    role_id: int,
) -> hikari.Role | None:
    cache = state.client.app.cache
    if cache is not None:
        cached = cache.get_role(role_id)
        if cached is not None:
            return cached
    try:
        return await state.client.app.rest.fetch_role(guild_id, role_id)
    except hikari.NotFoundError:
        return None


async def fetch_channel(
    state: TimeoutState,
    channel_id: hikari.Snowflakeish,
) -> hikari.GuildChannel | hikari.GuildThreadChannel:
    cache = state.client.app.cache
    if cache is not None:
        cached = cache.get_guild_channel(hikari.Snowflake(channel_id))
        if cached is not None:
            return cached
    channel = await state.client.app.rest.fetch_channel(channel_id)
    if isinstance(channel, hikari.GuildThreadChannel):
        return channel
    if isinstance(channel, hikari.GuildChannel):
        return channel
    msg = "Not a guild channel"
    raise TypeError(msg)


async def check_admin(ctx: arc.GatewayContext) -> arc.HookResult:
    state = get_state()
    if ctx.author.id in state.client.owner_ids:
        return arc.HookResult(abort=False)
    res_user: bool = ctx.author.id in _admin_user_ids
    member = ctx.member
    res_role = bool(member is not None and _has_any_admin_role(member.role_ids))
    if res_user or res_role:
        return arc.HookResult(abort=False)
    await reply_err(
        state.client.app,
        ctx,
        "Insufficient permissions.",
    )
    return arc.HookResult(abort=True)


async def check_guild_moderator(ctx: arc.GatewayContext) -> arc.HookResult:
    state = get_state()
    res_user: bool = ctx.author.id in _guild_moderator_user_ids
    member = ctx.member
    res_role = bool(
        member is not None and _has_any_guild_moderator_role(member.role_ids),
    )
    if res_user or res_role:
        return arc.HookResult(abort=False)
    await reply_err(
        state.client.app,
        ctx,
        "Insufficient permissions.",
    )
    return arc.HookResult(abort=True)


async def check_channel_moderator(ctx: arc.GatewayContext) -> arc.HookResult:
    state = get_state()
    res_user: bool = ctx.author.id in _guild_moderator_user_ids
    member = ctx.member
    res_role = bool(
        member is not None and _has_any_guild_moderator_role(member.role_ids),
    )
    if res_user or res_role:
        return arc.HookResult(abort=False)
    channel = await fetch_channel(state, ctx.channel_id)
    channel_id: int = fetch_parent_id(channel)
    cmod_user = ctx.author.id in _channel_moderator_user_ids.get(channel_id, set())
    res_user = cmod_user
    res_role = bool(
        member is not None and _has_any_channel_moderator_role(channel_id, member.role_ids),
    )
    if res_user or res_role:
        return arc.HookResult(abort=False)
    await reply_err(
        state.client.app,
        ctx,
        "Insufficient permissions.",
    )
    return arc.HookResult(abort=True)


class TimeoutState:
    def __init__(
        self,
        client: arc.GatewayClient,
    ) -> None:
        self.client = client
        self.startup_flag = False
        self.lock_db: asyncio.Lock = asyncio.Lock()
        self._shutdown = False
        self._shutdown_lock = asyncio.Lock()

    @property
    def miru_client(self) -> miru.Client:
        return get_miru()

    async def async_init(self) -> None:
        global admins
        global channel_moderators
        global guild_moderators
        global timeout_members
        admin_records = await asyncio.to_thread(AdminDB.get_databases, env)
        channel_moderator_records = await asyncio.to_thread(
            ModeratorDB.get_databases,
            env,
        )
        global_moderator_records = await asyncio.to_thread(
            GlobalModeratorDB.get_databases,
            env,
        )
        prisoner_records = await asyncio.to_thread(TimeoutMemberDB.get_databases, env)
        setting_records = await asyncio.to_thread(SettingDB.get_databases, env)
        admins = [
            Admin(int(record["id"]), int(record["type"]))
            for record in admin_records
            if "id" in record and "type" in record
        ]
        _rebuild_admin_index()
        channel_moderators = [
            ChannelModerator(
                int(record["id"]),
                int(record["type"]),
                int(record["channel_id"]),
            )
            for record in channel_moderator_records
            if "id" in record and "type" in record and "channel_id" in record
        ]
        _rebuild_channel_moderator_index()
        guild_moderators = [
            GuildModerator(int(record["id"]), int(record["type"]))
            for record in global_moderator_records
            if "id" in record and "type" in record
        ]
        _rebuild_guild_moderator_index()
        timeout_members = [
            TimeoutMember(
                int(record["id"]),
                _ensure_utc(record["release_datetime"]),
                int(record["channel_id"]),
            )
            for record in prisoner_records
            if (
                "id" in record
                and "channel_id" in record
                and isinstance(record.get("release_datetime"), datetime.datetime)
            )
        ]
        _rebuild_timeout_index()
        settings.clear()
        for setting_type in SettingType:
            settings.append(Config(setting_type, 600, None))
        for setting_record in setting_records:
            try:
                setting_type = SettingType(int(setting_record["type"]))
                primary_value = int(setting_record["primary_value"])
            except (KeyError, TypeError, ValueError):
                continue
            secondary_value_raw = setting_record.get("secondary_value")
            secondary_value = (
                secondary_value_raw
                if isinstance(secondary_value_raw, str) or secondary_value_raw is None
                else str(secondary_value_raw)
            )
            _upsert_setting_cache(
                Config(setting_type, primary_value, secondary_value),
            )
        _rebuild_settings_index()
        await self.async_start()

    async def async_start(self) -> None:
        if self.startup_flag:
            return
        self.startup_flag = True
        app = typing.cast("hikari.GatewayBot", self.client.app)
        await app.wait_for(hikari.StartedEvent, timeout=None)
        current_datetime = _now_utc()
        for timeout_member in tuple(timeout_members):
            duration_minutes: float = (
                _ensure_utc(timeout_member.release_datetime) - current_datetime
            ).total_seconds() / 60
            if duration_minutes <= 0:
                await self.release(timeout_member)
            else:
                self._register_prisoner_task(
                    timeout_member,
                    self.release_prisoner_task(
                        duration_minutes=duration_minutes,
                        timeout_member=timeout_member,
                    ),
                )

    def drop(self) -> None:
        async def _do_shutdown() -> None:
            async with self._shutdown_lock:
                if self._shutdown:
                    return
                self._shutdown = True
                for task in tuple(timeout_tasks.values()):
                    task.cancel()
                timeout_tasks.clear()
                database.close()

        asyncio.create_task(_do_shutdown())

    def init_task(self, state: TimeoutState) -> asyncio.Task[None]:
        return asyncio.create_task(state.async_init())

    async def update_global_setting(
        self,
        setting_type: SettingType,
        primary_value: int,
        secondary_value: str | None = None,
    ) -> None:
        config = Config(setting_type, primary_value, secondary_value)
        _upsert_setting_cache(config)
        async with self.lock_db:
            await asyncio.to_thread(
                SettingDB.upsert_setting,
                env,
                setting_type,
                primary_value,
                secondary_value,
            )

    def _register_prisoner_task(
        self,
        timeout_member: TimeoutMember,
        awaitable: Coroutine[typing.Any, typing.Any, None],
    ) -> asyncio.Task[None]:
        key = timeout_member.to_tuple()
        previous = timeout_tasks.pop(key, None)
        if previous is not None:
            previous.cancel()
        task = asyncio.create_task(awaitable)
        timeout_tasks[key] = task

        def _done(
            completed_task: asyncio.Task[None],
            task_key: tuple[int, int] = key,
        ) -> None:
            timeout_tasks.pop(task_key, None)
            with contextlib.suppress(asyncio.CancelledError):
                if completed_task.exception() is not None:
                    logger.exception(
                        "Failed to release timeout for user=%s channel=%s",
                        task_key[0],
                        task_key[1],
                        exc_info=completed_task.exception(),
                    )

        task.add_done_callback(_done)
        return task

    def _cancel_prisoner_task(self, timeout_member: TimeoutMember) -> None:
        task = timeout_tasks.pop(timeout_member.to_tuple(), None)
        if task is not None:
            task.cancel()

    async def release(
        self,
        timeout_member: TimeoutMember,
        ctx: arc.Context | miru.abc.Context | None = None,
    ) -> None:
        existing_timeout_member = _get_timeout_member(
            timeout_member.id,
            timeout_member.channel_id,
        )
        if existing_timeout_member is None:
            if ctx is not None:
                await reply_err(
                    self.client.app,
                    ctx,
                    "Failed to find timed out member.",
                )
            return
        timeout_member = existing_timeout_member
        try:
            user = await self.client.app.rest.fetch_user(timeout_member.id)
        except hikari.NotFoundError:
            user = None
        if timeout_member.is_global:
            try:
                guild_id = ctx.guild_id if ctx is not None else None
                if guild_id is None:
                    guild_setting = _setting(SettingType.LOG_CHANNEL).secondary_value
                    if guild_setting is not None:
                        guild_id = hikari.Snowflake(int(guild_setting))
                if guild_id is not None:
                    member = await fetch_member(self, int(guild_id), timeout_member.id)
                    if member is not None:
                        await member.edit(communication_disabled_until=None)
            except hikari.ForbiddenError:
                if ctx is not None:
                    await reply_err(
                        self.client.app,
                        ctx,
                        "Failed to obtain required permissions.",
                    )
                return
            except hikari.NotFoundError:
                pass
        else:
            channel = await fetch_channel(self, timeout_member.channel_id)
            try:
                await self.client.app.rest.delete_permission_overwrite(
                    channel.id,
                    timeout_member.id,
                    reason=(
                        "Member "
                        f"{(user.username if user is not None else timeout_member.id)}({timeout_member.id}) "
                        f"is released from Channel {channel.name} timeout."
                    ),
                )
            except hikari.ForbiddenError:
                if ctx is not None:
                    await reply_err(
                        self.client.app,
                        ctx,
                        "Failed to obtain required permissions.",
                    )
                return
            except hikari.NotFoundError:
                pass
        _delete_timeout_member(timeout_member)
        self._cancel_prisoner_task(timeout_member)
        async with self.lock_db:
            await asyncio.to_thread(
                TimeoutMemberDB.delete_prisoner,
                env,
                timeout_member.id,
                timeout_member.channel_id,
            )
        if ctx is not None:
            guild_id = ctx.guild_id
            if guild_id is None:
                member = None
            else:
                member = await fetch_member(self, int(guild_id), timeout_member.id)
            if timeout_member.is_global:
                msg: str = f"Released {user_display(member, timeout_member.id)} from the guild."
            else:
                channel = await fetch_channel(self, timeout_member.channel_id)
                msg: str = f"Released {user_display(member, timeout_member.id)} from {channel.mention}."
            await reply_ok(self.client.app, ctx, msg)
            await self.send_log(msg, Color.INFO)
        elif _setting(SettingType.LOG_CHANNEL).secondary_value is not None:
            secondary_value = _setting(SettingType.LOG_CHANNEL).secondary_value
            if secondary_value is None:
                return
            with contextlib.suppress(
                ValueError,
                hikari.ForbiddenError,
                hikari.NotFoundError,
            ):
                guild_id = int(secondary_value)
                g = await self.client.app.rest.fetch_guild(guild_id)
                member = await fetch_member(self, int(g.id), timeout_member.id)
                msg = f"Released {user_display(member, timeout_member.id)}"
                await self.send_log(msg, Color.INFO)
                await self.client.app.rest.create_message(
                    _setting(SettingType.LOG_CHANNEL).primary_value,
                    embed=hikari.Embed(
                        title="Timeout",
                        description=msg,
                        color=Color.INFO,
                    ),
                )

    def check_timeout_member(
        self,
        prisoner_member: hikari.Member,
        duration_minutes: int,
        channel: hikari.GuildChannel | hikari.GuildThreadChannel,
    ) -> tuple[bool, TimeoutMember]:
        channel_id: int = fetch_parent_id(channel)
        existing = _get_timeout_member(prisoner_member.id, channel_id)
        if existing is not None:
            return True, existing
        timeout_member = TimeoutMember(
            prisoner_member.id,
            _now_utc() + datetime.timedelta(minutes=duration_minutes),
            channel_id,
        )
        return False, timeout_member

    async def timeout_channel(
        self,
        prisoner_member: hikari.Member,
        duration_minutes: int,
        channel: hikari.GuildChannel | hikari.GuildThreadChannel,
        ctx: arc.Context | miru.abc.Context | None = None,
        reason: str = "",
    ) -> bool:
        async def respond(message: str) -> None:
            if ctx is not None:
                await reply_err(self.client.app, ctx, message)

        if duration_minutes <= 0:
            await respond("Duration must exceed 0 minutes.")
            return False
        existed, timeout_member = self.check_timeout_member(
            prisoner_member,
            duration_minutes,
            channel,
        )
        if existed:
            await respond(
                f"{prisoner_member.mention} already timed out in {channel.mention}.",
            )
            return False

        channel_id: int = fetch_parent_id(channel)
        member_role_ids: frozenset[int] = frozenset(prisoner_member.role_ids)

        if prisoner_member.id in _channel_moderator_user_ids.get(
            channel_id,
            set(),
        ) or _has_any_channel_moderator_role(
            channel_id,
            list(member_role_ids),
        ):
            await respond("Failed to target channel moderator.")
            return False

        if prisoner_member.id in _guild_moderator_user_ids or _has_any_guild_moderator_role(list(member_role_ids)):
            await respond("Failed to target guild moderator.")
            return False

        if prisoner_member.id in _admin_user_ids or _has_any_admin_role(
            list(member_role_ids),
        ):
            await respond("Failed to target admin.")
            return False

        minute_limit: int = _setting(SettingType.MINUTE_LIMIT).primary_value
        if duration_minutes > minute_limit:
            await respond(f"Duration cannot exceed {minute_limit} minutes.")
            return False

        deny_permissions = (
            hikari.Permissions.SEND_MESSAGES
            | hikari.Permissions.SEND_MESSAGES_IN_THREADS
            | hikari.Permissions.SEND_TTS_MESSAGES
            | hikari.Permissions.SEND_VOICE_MESSAGES
            | hikari.Permissions.ADD_REACTIONS
            | hikari.Permissions.ATTACH_FILES
            | hikari.Permissions.EMBED_LINKS
            | hikari.Permissions.MENTION_ROLES
            | hikari.Permissions.USE_EXTERNAL_EMOJIS
            | hikari.Permissions.USE_EXTERNAL_STICKERS
            | hikari.Permissions.USE_EXTERNAL_SOUNDS
            | hikari.Permissions.USE_EXTERNAL_APPS
            | hikari.Permissions.SEND_POLLS
            | hikari.Permissions.CREATE_PUBLIC_THREADS
            | hikari.Permissions.CREATE_PRIVATE_THREADS
            | hikari.Permissions.MANAGE_THREADS
            | hikari.Permissions.CONNECT
            | hikari.Permissions.SPEAK
            | hikari.Permissions.PRIORITY_SPEAKER
            | hikari.Permissions.STREAM
            | hikari.Permissions.USE_VOICE_ACTIVITY
            | hikari.Permissions.MUTE_MEMBERS
            | hikari.Permissions.DEAFEN_MEMBERS
            | hikari.Permissions.MOVE_MEMBERS
            | hikari.Permissions.USE_SOUNDBOARD
            | hikari.Permissions.REQUEST_TO_SPEAK
            | hikari.Permissions.START_EMBEDDED_ACTIVITIES
            | hikari.Permissions.CREATE_EVENTS
            | hikari.Permissions.MANAGE_EVENTS
            | hikari.Permissions.CREATE_INSTANT_INVITE
            | hikari.Permissions.READ_MESSAGE_HISTORY
            | hikari.Permissions.PIN_MESSAGES
            | hikari.Permissions.MANAGE_MESSAGES
            | hikari.Permissions.MANAGE_CHANNELS
            | hikari.Permissions.MANAGE_WEBHOOKS
            | hikari.Permissions.MANAGE_ROLES
            # | hikari.Permissions.BYPASS_SLOWMODE
            | hikari.Permissions.MANAGE_GUILD_EXPRESSIONS
            | hikari.Permissions.CREATE_GUILD_EXPRESSIONS
        )

        target_channel: hikari.GuildChannel
        if isinstance(channel, hikari.GuildThreadChannel):
            target_channel = await fetch_parent_channel(self, channel)
        else:
            target_channel = channel

        try:
            await self.client.app.rest.edit_permission_overwrite(
                target_channel.id,
                prisoner_member.id,
                target_type=hikari.PermissionOverwriteType.MEMBER,
                allow=hikari.Permissions.NONE,
                deny=deny_permissions,
                reason=(
                    f"Timed out member {name_display(prisoner_member)}({prisoner_member.id}) for {duration_minutes} minutes in {target_channel.name}. Reason: {reason}"
                ),
            )
        except hikari.ForbiddenError:
            await respond("Failed to obtain required permissions.")
            return False

        _upsert_timeout_member(timeout_member)
        async with self.lock_db:
            await asyncio.to_thread(
                TimeoutMemberDB.add,
                env,
                timeout_member.id,
                timeout_member.channel_id,
                timeout_member.release_datetime,
            )

        if ctx is not None:
            await reply_ok(
                self.client.app,
                ctx,
                f"Timed out {prisoner_member.mention} for {duration_minutes} minutes. Reason: {reason}",
            )
        else:
            await self.client.app.rest.create_message(
                channel.id,
                f"Timed out {prisoner_member.mention} for {duration_minutes} minutes in the {channel.mention}. Reason: {reason}",
                flags=message_flags(silent=True),
            )

        await self.send_log(
            f"Timed out {prisoner_member.mention} for {duration_minutes} minutes in {channel.mention}. Reason: {reason}",
            Color.WARNING,
        )

        self._register_prisoner_task(
            timeout_member,
            self.release_prisoner_task(
                duration_minutes=duration_minutes,
                timeout_member=timeout_member,
                ctx=ctx,
            ),
        )
        return True

    async def timeout_guild(
        self,
        prisoner_member: hikari.Member,
        duration_minutes: int,
        ctx: arc.Context | miru.abc.Context | None = None,
        reason: str = "",
    ) -> bool:
        async def respond(message: str) -> None:
            if ctx is not None:
                await reply_err(self.client.app, ctx, message)

        if duration_minutes <= 0:
            await respond("Duration must exceed 0 minutes.")
            return False

        if prisoner_member.id in _global_timeout_user_ids:
            await respond(f"{prisoner_member.mention} already timed out in the guild.")
            return False

        member_role_ids = frozenset(prisoner_member.role_ids)

        if prisoner_member.id in _guild_moderator_user_ids or _has_any_guild_moderator_role(list(member_role_ids)):
            await respond("Failed to target guild moderator.")
            return False

        if prisoner_member.id in _admin_user_ids or _has_any_admin_role(
            list(member_role_ids),
        ):
            await respond("Failed to target admin.")
            return False

        minute_limit = _setting(SettingType.MINUTE_LIMIT).primary_value
        if duration_minutes > minute_limit:
            await respond(f"Duration cannot exceed {minute_limit} minutes.")
            return False

        timeout_member = TimeoutMember(
            prisoner_member.id,
            _now_utc() + datetime.timedelta(minutes=duration_minutes),
            -1,
        )

        try:
            await prisoner_member.edit(
                communication_disabled_until=timeout_member.release_datetime,
                reason=f"Timed out member {name_display(prisoner_member)}({prisoner_member.id}) for {duration_minutes} minutes in the guild. Reason: {reason}",
            )
        except hikari.ForbiddenError:
            await respond("Failed to obtain required permissions.")
            return False

        _upsert_timeout_member(timeout_member)
        async with self.lock_db:
            await asyncio.to_thread(
                TimeoutMemberDB.add,
                env,
                timeout_member.id,
                timeout_member.channel_id,
                timeout_member.release_datetime,
            )

        if ctx is not None:
            await reply_ok(
                self.client.app,
                ctx,
                f"Timed out {prisoner_member.mention} for {duration_minutes} minutes. Reason: {reason}",
            )
        await self.send_log(
            f"Timed out {prisoner_member.mention} for {duration_minutes} minutes. Reason: {reason}",
            Color.WARNING,
        )

        self._register_prisoner_task(
            timeout_member,
            self.release_prisoner_task(
                duration_minutes=duration_minutes,
                timeout_member=timeout_member,
                ctx=ctx,
            ),
        )
        return True

    async def release_prisoner_task(
        self,
        duration_minutes: float,
        timeout_member: TimeoutMember,
        ctx: arc.Context | miru.abc.Context | None = None,
    ) -> None:
        try:
            await asyncio.sleep(duration_minutes * 60.0)
            await self.release(timeout_member=timeout_member)
            if ctx is not None and ctx.guild_id is not None:
                member = await fetch_member(self, int(ctx.guild_id), timeout_member.id)
                if member is not None:
                    await self.client.app.rest.create_message(
                        ctx.channel_id,
                        f"{member.mention} is released.",
                        flags=message_flags(silent=True),
                    )
        except asyncio.CancelledError:
            pass

    async def send_log(self, message: str, colour: int = 0) -> None:
        channel_config: Config = _setting(SettingType.LOG_CHANNEL)
        if channel_config.secondary_value is None:
            return
        try:
            channel_id = int(channel_config.primary_value)
        except (TypeError, ValueError):
            return
        embed = await reply_embed(
            self.client.app,
            "Timeout",
            message,
            Color(colour),
        )
        try:
            await self.client.app.rest.create_message(channel_id, embed=embed)
        except (hikari.ForbiddenError, hikari.NotFoundError):
            return


class AdminUserSelectView(miru.View):
    def __init__(self, state: TimeoutState) -> None:
        super().__init__(timeout=300)
        self.state = state

    @miru.user_select(
        custom_id="admin_user",
        placeholder="Select admin user",
        max_values=25,
    )
    async def select_user(self, ctx: miru.ViewContext, select: miru.UserSelect) -> None:
        await handle_admin_select_component(
            self.state,
            ctx,
            select.values,
            MRCTType.USER,
        )


class AdminRoleSelectView(miru.View):
    def __init__(self, state: TimeoutState) -> None:
        super().__init__(timeout=300)
        self.state = state

    @miru.role_select(
        custom_id="admin_role",
        placeholder="Select admin role",
        max_values=25,
    )
    async def select_role(self, ctx: miru.ViewContext, select: miru.RoleSelect) -> None:
        await handle_admin_select_component(
            self.state,
            ctx,
            select.values,
            MRCTType.ROLE,
        )


class ChannelModeratorUserSelectView(miru.View):
    def __init__(
        self,
        state: TimeoutState,
        channel: hikari.GuildChannel,
    ) -> None:
        super().__init__(timeout=300)
        self.state = state
        self.channel = channel

    @miru.user_select(
        custom_id="channel_moderator_user",
        placeholder="Select channel moderator user",
        max_values=25,
    )
    async def select_user(self, ctx: miru.ViewContext, select: miru.UserSelect) -> None:
        await handle_others_select_component(
            self.state,
            ctx,
            select.values,
            False,
            MRCTType.USER,
            channel=self.channel,
        )


class ChannelModeratorRoleSelectView(miru.View):
    def __init__(
        self,
        state: TimeoutState,
        channel: hikari.GuildChannel,
    ) -> None:
        super().__init__(timeout=300)
        self.state = state
        self.channel = channel

    @miru.role_select(
        custom_id="channel_moderator_role",
        placeholder="Select channel moderator role",
        max_values=25,
    )
    async def select_role(self, ctx: miru.ViewContext, select: miru.RoleSelect) -> None:
        await handle_others_select_component(
            self.state,
            ctx,
            select.values,
            False,
            MRCTType.ROLE,
            channel=self.channel,
        )


class GuildModeratorUserSelectView(miru.View):
    def __init__(self, state: TimeoutState) -> None:
        super().__init__(timeout=300)
        self.state = state

    @miru.user_select(
        custom_id="guild_moderator_user",
        placeholder="Select guild moderator user",
        max_values=25,
    )
    async def select_user(self, ctx: miru.ViewContext, select: miru.UserSelect) -> None:
        await handle_others_select_component(
            self.state,
            ctx,
            select.values,
            True,
            MRCTType.USER,
        )


class GuildModeratorRoleSelectView(miru.View):
    def __init__(self, state: TimeoutState) -> None:
        super().__init__(timeout=300)
        self.state = state

    @miru.role_select(
        custom_id="guild_moderator_role",
        placeholder="Select guild moderator role",
        max_values=25,
    )
    async def select_role(self, ctx: miru.ViewContext, select: miru.RoleSelect) -> None:
        await handle_others_select_component(
            self.state,
            ctx,
            select.values,
            True,
            MRCTType.ROLE,
        )


async def handle_admin_select_component(
    state: TimeoutState,
    ctx: miru.ViewContext,
    values: Sequence[hikari.User | hikari.Member | hikari.Role],
    gaType: MRCTType,
) -> None:
    msg_to_send: str = "Added admin "
    msg_to_send += "as user:" if gaType == MRCTType.USER else "as role:"

    for value in values:
        selected_id: int | None = None
        if gaType == MRCTType.USER:
            user_value = typing.cast("hikari.User | hikari.Member", value)
            if is_bot(user_value):
                continue
            user_display_name = name_display(user_value)
            mention = user_value.mention
            selected_id = user_value.id
        elif gaType == MRCTType.ROLE:
            role_value = typing.cast("hikari.Role", value)
            user_display_name = role_value.name
            mention = role_value.mention
            selected_id = role_value.id
        if selected_id is None:
            continue

        ga_to_add = Admin(selected_id, gaType)
        if _add_admin(ga_to_add):
            async with state.lock_db:
                await asyncio.to_thread(
                    AdminDB.add,
                    env,
                    ga_to_add.id,
                    ga_to_add.type,
                )
            msg_to_send += f"\n- {user_display_name} {mention}"

    await ctx.edit_response(
        content="Configured admin",
        components=[],
    )
    await state.send_log(msg_to_send, Color.INFO)


async def handle_others_select_component(
    state: TimeoutState,
    ctx: miru.ViewContext,
    values: Sequence[hikari.User | hikari.Member | hikari.Role],
    is_global_moderator: bool,
    gaType: MRCTType,
    channel: hikari.GuildChannel | None = None,
) -> None:
    if not is_global_moderator:
        if channel is None:
            channel = await fetch_channel(state, ctx.channel_id)
            channel = await fetch_parent_channel(state, channel)
        assert channel is not None
    channel_mention = channel.mention if channel is not None else ""

    if is_global_moderator:
        msg_to_send: str = "Added guild moderator "
    else:
        msg_to_send: str = f"Added {channel_mention} channel moderator "

    msg_to_send += "as user:" if gaType == MRCTType.USER else "as role:"

    for value in values:
        selected_id: int | None = None
        if gaType == MRCTType.USER:
            user_value = typing.cast("hikari.User | hikari.Member", value)
            if is_bot(user_value):
                continue
            user_display_name = name_display(user_value)
            mention = user_value.mention
            selected_id = user_value.id
        elif gaType == MRCTType.ROLE:
            role_value = typing.cast("hikari.Role", value)
            user_display_name = role_value.name
            mention = role_value.mention
            selected_id = role_value.id
        if selected_id is None:
            continue

        if is_global_moderator:
            gm_to_add = GuildModerator(selected_id, gaType)
            if _add_guild_moderator(gm_to_add):
                async with state.lock_db:
                    await asyncio.to_thread(
                        GlobalModeratorDB.add,
                        env,
                        gm_to_add.id,
                        gm_to_add.type,
                    )
                msg_to_send += f"\n- {user_display_name} {mention}"
        else:
            if channel is None:
                return
            cm_to_add = ChannelModerator(
                selected_id,
                gaType,
                channel.id,
            )
            if _add_channel_moderator(cm_to_add):
                async with state.lock_db:
                    await asyncio.to_thread(
                        ModeratorDB.add,
                        env,
                        cm_to_add.id,
                        cm_to_add.type,
                        cm_to_add.channel_id,
                    )
                msg_to_send += f"\n- {user_display_name} {mention}"

    await ctx.edit_response(
        content=(
            f"{'Guild Moderator' if is_global_moderator else 'Channel Moderator'}"
            f" {'user' if gaType == MRCTType.USER else 'role'} configured"
        ),
        components=[],
    )
    await state.send_log(msg_to_send, Color.INFO)


plugin = arc.GatewayPlugin("Timeout")
group = plugin.include_slash_group(
    "timeout",
    "Timeout",
)
group_setting = group.include_subgroup(
    "setting",
    "Settings commands",
)
group_channel = group.include_subgroup(
    "channel",
    "Channel commands",
)
group_guild = group.include_subgroup(
    "guild",
    "Guild commands",
)

_state: TimeoutState | None = None


def get_state() -> TimeoutState:
    if _state is None:
        msg = "Failed to initialize"
        raise RuntimeError(msg)
    return _state


@plugin.listen(hikari.MemberCreateEvent)
async def member_create(event: hikari.MemberCreateEvent) -> None:
    state = get_state()
    cdt = _now_utc()
    cps = _get_timeout_members_for_user(event.member.id)
    for cp in cps:
        duration_minutes_float: float = (_ensure_utc(cp.release_datetime) - cdt).total_seconds() / 60
        duration_minutes: int = math.ceil(duration_minutes_float) if duration_minutes_float > 0 else 1
        await state.release(cp)

        if cp.is_global:
            await state.timeout_guild(
                event.member,
                duration_minutes,
                reason="Re-jail escaped member",
            )
        else:
            channel = await fetch_channel(state, cp.channel_id)
            await state.timeout_channel(
                event.member,
                duration_minutes,
                channel,
                reason="Re-jail escaped member",
            )


@group_setting.include
@arc.with_hook(check_admin)
@arc.slash_subcommand(
    "limit",
    description="Set the minute timeout limitation",
)
async def cmd_setting_limit(
    ctx: arc.GatewayContext,
    minute: arc.Option[
        int,
        arc.IntParams(
            name="minute",
            description="The timeout limit",
            min=1,
        ),
    ],
) -> None:
    state = get_state()
    await state.update_global_setting(SettingType.MINUTE_LIMIT, minute)
    await reply_ok(state.client.app, ctx, f"Set timeout limit to {minute} minutes.")
    await state.send_log(f"Set timeout limit to {minute} minutes.")


@group_setting.include
@arc.with_hook(check_admin)
@arc.slash_subcommand(
    "log",
    description="Set the channel to output log",
)
async def cmd_setting_log(
    ctx: arc.GatewayContext,
    channel: arc.Option[
        hikari.PartialChannel,
        arc.ChannelParams(
            name="channel",
            description="The channel to output the logs",
        ),
    ],
) -> None:
    state = get_state()
    if ctx.guild_id is None:
        await reply_err(state.client.app, ctx, "Failed to provide guild context.")
        return
    if channel.type not in {
        hikari.ChannelType.GUILD_TEXT,
        hikari.ChannelType.GUILD_NEWS,
        hikari.ChannelType.GUILD_NEWS_THREAD,
        hikari.ChannelType.GUILD_PUBLIC_THREAD,
        hikari.ChannelType.GUILD_PRIVATE_THREAD,
    }:
        await reply_err(
            state.client.app,
            ctx,
            "Failed to send messages in specified channel.",
        )
        return
    await state.update_global_setting(
        SettingType.LOG_CHANNEL,
        int(channel.id),
        str(ctx.guild_id),
    )
    await reply_ok(
        state.client.app,
        ctx,
        f"Configured log channel to {mention_channel(int(channel.id))}.",
    )
    await state.send_log(
        f"Configured log channel to {mention_channel(int(channel.id))}.",
    )


@group_setting.include
@arc.with_hook(check_admin)
@arc.slash_subcommand(
    "insert",
    description="Set admin or moderator",
)
async def cmd_setting_insert(
    ctx: arc.GatewayContext,
    target: arc.Option[
        str,
        arc.StrParams(
            name="target",
            description="Select admin or moderator",
            choices={
                "admin": "admin",
                "guild_moderator": "guild_moderator",
                "channel_moderator": "channel_moderator",
            },
        ),
    ],
    entity: arc.Option[
        int,
        arc.IntParams(
            name="entity",
            description="Type of the target entity",
            choices={"User": MRCTType.USER, "Role": MRCTType.ROLE},
        ),
    ],
) -> None:
    state = get_state()

    async def respond_with_bound_view(content: str, view: miru.View) -> None:
        response_obj = await ctx.respond(
            content=content,
            components=view.build(),
            flags=hikari.MessageFlag.EPHEMERAL,
        )
        message = await bind_view_to_response(
            response_obj=response_obj,
            miru_client=state.miru_client,
            view=view,
        )
        if message is None:
            msg = "Failed to retrieve interaction response message for miru view binding."
            raise RuntimeError(msg)

    if target == "admin":
        if entity == MRCTType.USER:
            view = AdminUserSelectView(state)
            await respond_with_bound_view("Select admin user:", view)
        elif entity == MRCTType.ROLE:
            view = AdminRoleSelectView(state)
            await respond_with_bound_view("Select admin role:", view)
    elif target == "guild_moderator":
        if entity == MRCTType.USER:
            view = GuildModeratorUserSelectView(state)
            await respond_with_bound_view("Select guild moderator user:", view)
        elif entity == MRCTType.ROLE:
            view = GuildModeratorRoleSelectView(state)
            await respond_with_bound_view("Select guild moderator role:", view)
    elif target == "channel_moderator":
        channel = await fetch_channel(state, ctx.channel_id)
        channel = await fetch_parent_channel(state, channel)
        if entity == MRCTType.USER:
            view = ChannelModeratorUserSelectView(state, channel)
            await respond_with_bound_view(
                f"Select {channel.name} channel moderator user:",
                view,
            )
        elif entity == MRCTType.ROLE:
            view = ChannelModeratorRoleSelectView(state, channel)
            await respond_with_bound_view(
                f"Select {channel.name} channel moderator role:",
                view,
            )


async def autocomplete_invert_user(
    ctx: arc.AutocompleteData[arc.GatewayClient, str],
) -> Mapping[str, str]:
    option = ctx.focused_option
    option_input = str(option.value) if option and option.value is not None else ""
    state = get_state()
    guild_id = ctx.interaction.guild_id
    if guild_id is None:
        return {}

    target_type_val = next(
        (opt.value for opt in ctx.interaction.options if opt.name == "target"),
        None,
    )
    if target_type_val is None:
        return {}

    guild_id_int = int(guild_id)
    user_ids: list[int] = []

    if target_type_val == "admin":
        user_ids = list(_admin_user_ids)
    elif target_type_val == "guild_moderator":
        user_ids = list(_guild_moderator_user_ids)
    elif target_type_val == "channel_moderator":
        channel_id = ctx.interaction.channel_id
        if channel_id is None:
            return {}
        channel = await fetch_parent_channel(
            state,
            await fetch_channel(state, channel_id),
        )
        user_ids = list(_channel_moderator_user_ids.get(channel.id, set()))

    if not user_ids:
        return {}

    members = await asyncio.gather(
        *(fetch_member(state, guild_id_int, uid) for uid in user_ids),
        return_exceptions=True,
    )
    valid_members = [m for m in members if isinstance(m, hikari.Member)]

    if option_input:
        valid_members = [m for m in valid_members if option_input in name_display(m) or option_input in m.username]

    return {name_display(m): str(m.id) for m in valid_members[:25]}


async def autocomplete_invert_role(
    ctx: arc.AutocompleteData[arc.GatewayClient, str],
) -> Mapping[str, str]:
    option = ctx.focused_option
    option_input = str(option.value) if option and option.value is not None else ""
    state = get_state()
    guild_id = ctx.interaction.guild_id
    if guild_id is None:
        return {}

    target_type_val = next(
        (opt.value for opt in ctx.interaction.options if opt.name == "target"),
        None,
    )
    if target_type_val is None:
        return {}

    guild_id_int = int(guild_id)
    role_ids: list[int] = []

    if target_type_val == "admin":
        role_ids = list(_admin_role_ids)
    elif target_type_val == "guild_moderator":
        role_ids = list(_guild_moderator_role_ids)
    elif target_type_val == "channel_moderator":
        channel_id = ctx.interaction.channel_id
        if channel_id is None:
            return {}
        channel = await fetch_parent_channel(
            state,
            await fetch_channel(state, channel_id),
        )
        role_ids = list(_channel_moderator_role_ids.get(channel.id, set()))

    if not role_ids:
        return {}

    roles = await asyncio.gather(
        *(fetch_role(state, guild_id_int, rid) for rid in role_ids),
        return_exceptions=True,
    )
    valid_roles = [r for r in roles if isinstance(r, hikari.Role)]

    if option_input:
        valid_roles = [r for r in valid_roles if option_input in r.name]

    return {r.name: str(r.id) for r in valid_roles[:25]}


@group_setting.include
@arc.with_hook(check_admin)
@arc.slash_subcommand(
    "invert",
    description="Remove admin or moderator",
)
async def module_group_setting_invert(
    ctx: arc.GatewayContext,
    target: arc.Option[
        str,
        arc.StrParams(
            name="target",
            description="Select admin or moderator",
            choices={
                "admin": "admin",
                "guild_moderator": "guild_moderator",
                "channel_moderator": "channel_moderator",
            },
        ),
    ],
    user: arc.Option[
        str | None,
        arc.StrParams(
            name="user",
            description="The user to be removed",
            autocomplete_with=autocomplete_invert_user,
        ),
    ] = None,
    role: arc.Option[
        str | None,
        arc.StrParams(
            name="role",
            description="The role to be removed",
            autocomplete_with=autocomplete_invert_role,
        ),
    ] = None,
) -> None:
    state = get_state()
    if ctx.guild_id is None:
        await reply_err(state.client.app, ctx, "Failed to provide guild context.")
        return
    if user is None and role is None:
        await reply_err(
            state.client.app,
            ctx,
            "Failed to select user or role to remove",
        )
        return
    try:
        user_id = int(user) if user is not None else None
        role_id = int(role) if role is not None else None
    except ValueError:
        await reply_err(
            state.client.app,
            ctx,
            "Failed to parse input format",
        )
        return
    if target == "admin":
        async with state.lock_db:
            if user_id is not None:
                admin_entity = Admin(user_id, MRCTType.USER)
                member = await fetch_member(state, int(ctx.guild_id), admin_entity.id)
                admin_mention: str = user_display(member, admin_entity.id)
                if not _remove_admin(admin_entity):
                    await reply_err(
                        state.client.app,
                        ctx,
                        f"Failed to remove {admin_mention} - not an admin user.",
                    )
                    return
                await asyncio.to_thread(
                    AdminDB.delete_admin,
                    env,
                    admin_entity.id,
                    admin_entity.type,
                )
            if role_id is not None:
                admin_entity = Admin(role_id, MRCTType.ROLE)
                role_obj = await fetch_role(state, int(ctx.guild_id), admin_entity.id)
                admin_mention = role_display(role_obj, admin_entity.id)
                if not _remove_admin(admin_entity):
                    await reply_err(
                        state.client.app,
                        ctx,
                        f"Failed to remove {admin_mention} - not an admin role.",
                    )
                    return
                await asyncio.to_thread(
                    AdminDB.delete_admin,
                    env,
                    admin_entity.id,
                    admin_entity.type,
                )
        member = await fetch_member(state, int(ctx.guild_id), user_id) if user_id else None
        role_obj = await fetch_role(state, int(ctx.guild_id), role_id) if role_id else None
        await reply_ok(
            state.client.app,
            ctx,
            (
                "Removed admins:\n"
                f"{'- ' + user_display(member, user_id) if user_id is not None else ''}\n"
                f"{'- ' + role_display(role_obj, role_id) if role_id is not None else ''}"
            ),
        )
        await state.send_log(
            (
                "Removed admins:\n"
                f"{'- ' + user_display(member, user_id) if user_id is not None else ''}\n"
                f"{'- ' + role_display(role_obj, role_id) if role_id is not None else ''}"
            ),
            Color.WARNING,
        )
    elif target == "guild_moderator":
        async with state.lock_db:
            if user_id is not None:
                gm = GuildModerator(user_id, MRCTType.USER)
                member = await fetch_member(state, int(ctx.guild_id), gm.id)
                gm_mention: str = user_display(member, gm.id)
                if not _remove_guild_moderator(gm):
                    await reply_err(
                        state.client.app,
                        ctx,
                        f"Failed to remove {gm_mention} - not a guild moderator user.",
                    )
                    return
                await asyncio.to_thread(
                    GlobalModeratorDB.delete_global_moderator,
                    env,
                    gm.id,
                    gm.type,
                )
            if role_id is not None:
                gm = GuildModerator(role_id, MRCTType.ROLE)
                role_obj = await fetch_role(state, int(ctx.guild_id), gm.id)
                gm_mention = role_display(role_obj, gm.id)
                if not _remove_guild_moderator(gm):
                    await reply_err(
                        state.client.app,
                        ctx,
                        f"Failed to remove {gm_mention} - not a guild moderator role.",
                    )
                    return
                await asyncio.to_thread(
                    GlobalModeratorDB.delete_global_moderator,
                    env,
                    gm.id,
                    gm.type,
                )
        member = await fetch_member(state, int(ctx.guild_id), user_id) if user_id else None
        role_obj = await fetch_role(state, int(ctx.guild_id), role_id) if role_id else None
        await reply_ok(
            state.client.app,
            ctx,
            (
                "Removed guild moderators:\n"
                f"{'- ' + user_display(member, user_id) if user_id is not None else ''}\n"
                f"{'- ' + role_display(role_obj, role_id) if role_id is not None else ''}"
            ),
        )
        await state.send_log(
            (
                "Removed guild moderators:\n"
                f"{'- ' + user_display(member, user_id) if user_id is not None else ''}\n"
                f"{'- ' + role_display(role_obj, role_id) if role_id is not None else ''}"
            ),
            Color.WARNING,
        )
    elif target == "channel_moderator":
        channel = await fetch_channel(state, ctx.channel_id)
        channel = await fetch_parent_channel(state, channel)
        async with state.lock_db:
            if user_id is not None:
                cm = ChannelModerator(user_id, MRCTType.USER, channel.id)
                member = await fetch_member(state, int(ctx.guild_id), cm.id)
                cm_mention: str = user_display(member, cm.id)
                if not _remove_channel_moderator(cm):
                    await reply_err(
                        state.client.app,
                        ctx,
                        f"Failed to remove {cm_mention} - not moderating {channel.mention}.",
                    )
                    return
                await asyncio.to_thread(
                    ModeratorDB.delete_moderator,
                    env,
                    cm.id,
                    cm.type,
                    cm.channel_id,
                )
            if role_id is not None:
                cm = ChannelModerator(role_id, MRCTType.ROLE, channel.id)
                role_obj = await fetch_role(state, int(ctx.guild_id), cm.id)
                cm_mention = role_display(role_obj, cm.id)
                if not _remove_channel_moderator(cm):
                    await reply_err(
                        state.client.app,
                        ctx,
                        f"Failed to remove {cm_mention} - not moderating {channel.mention}.",
                    )
                    return
                await asyncio.to_thread(
                    ModeratorDB.delete_moderator,
                    env,
                    cm.id,
                    cm.type,
                    cm.channel_id,
                )
        member = await fetch_member(state, int(ctx.guild_id), user_id) if user_id else None
        role_obj = await fetch_role(state, int(ctx.guild_id), role_id) if role_id else None
        await reply_ok(
            state.client.app,
            ctx,
            (
                f"Removed channel moderator in {channel.mention}:\n"
                f"{'- ' + user_display(member, user_id) if user_id is not None else ''}\n"
                f"{'- ' + role_display(role_obj, role_id) if role_id is not None else ''}"
            ),
        )
        await state.send_log(
            (
                f"Removed channel moderator in {channel.mention}:\n"
                f"{'- ' + user_display(member, user_id) if user_id is not None else ''}\n"
                f"{'- ' + role_display(role_obj, role_id) if role_id is not None else ''}"
            ),
            Color.WARNING,
        )


async def send_pagination(
    state: TimeoutState,
    ctx: arc.GatewayContext,
    text: str,
    *,
    page_size: int,
    timeout: int | None = None,
) -> None:
    @dataclasses.dataclass(frozen=True, slots=True)
    class Section:
        title: str | None
        content: str

    def parse_sections(content: str) -> list[Section]:
        sections: list[Section] = []
        for block in content.split("\n\n"):
            block = block.strip()
            if not block:
                continue
            lines = block.split("\n")
            if len(lines) > 1 and lines[0].endswith(":"):
                sections.append(
                    Section(lines[0].rstrip(":"), "\n".join(lines[1:]).strip()),
                )
            else:
                sections.append(Section(None, block))
        return sections

    def paginate_sections(
        sections: list[Section],
        max_chars: int,
    ) -> Generator[list[Section], None, None]:
        current_page: list[Section] = []
        current_chars = 0
        for section in sections:
            section_chars = len(section.content)
            if current_chars + section_chars > max_chars and current_page:
                yield current_page
                current_page = [section]
                current_chars = section_chars
            else:
                current_page.append(section)
                current_chars += section_chars
        if current_page:
            yield current_page

    def build_embed(sections: list[Section], title: str | None) -> hikari.Embed:
        embed = hikari.Embed(title=title, color=Color.INFO)
        for section in sections:
            if len(section.content) <= 1024 and len(embed.fields) < 25:
                embed.add_field(
                    name=section.title or "Information",
                    value=section.content or "No data",
                    inline=False,
                )
            else:
                embed.description = section.content
                break
        return embed

    size = max(1, page_size)
    lines = text.split("\n")
    title = None
    if lines and ":" in lines[0]:
        title = lines[0].split(":", 1)[0].strip()
        remaining_text = "\n".join(lines[1:]).strip()
        text = remaining_text or "No content to display"
    sections = parse_sections(text)
    pages: list[hikari.Embed] = []
    for page_sections in paginate_sections(sections, size):
        embed = build_embed(page_sections, title)
        if not embed.fields and not embed.description:
            embed.add_field(name="Information", value="No data available", inline=False)
        pages.append(embed)
    if not pages:
        embed = hikari.Embed(title=title, color=Color.INFO)
        embed.add_field(name="Information", value="No data available", inline=False)
        pages.append(embed)
    buttons: list[nav.NavItem] = [
        nav.PrevButton(),
        nav.StopButton(),
        nav.NextButton(),
    ]
    navigator = nav.navigator.NavigatorView(
        pages=pages,
        items=buttons,
        timeout=timeout if timeout is not None else 180,
        autodefer=True,
    )
    builder = await navigator.build_response_async(state.miru_client)
    await respond_with_builder_and_bind_view(
        ctx=ctx,
        builder=builder,
        miru_client=state.miru_client,
        view=navigator,
    )


@group_setting.include
@arc.slash_subcommand(
    "view",
    description="View admins, moderators, timed-out users, or all settings",
)
async def cmd_setting_view(
    ctx: arc.GatewayContext,
    view_type: arc.Option[
        str,
        arc.StrParams(
            name="view_type",
            description="Choose what to display",
            choices={
                "admin": "admin",
                "guild_moderator": "guild_moderator",
                "channel_moderator": "channel_moderator",
                "timeout_members": "timeout_members",
                "all": "all",
            },
        ),
    ],
) -> None:
    async def format_moderator_entry(
        state: TimeoutState,
        guild_id: int,
        entry: Admin | GuildModerator | ChannelModerator,
    ) -> list[str]:
        lines: list[str] = []
        if entry.type == MRCTType.USER:
            member = await fetch_member(state, guild_id, entry.id)
            lines.append(f"- User: {user_display(member, entry.id)}")
        elif entry.type == MRCTType.ROLE:
            role = await fetch_role(state, guild_id, entry.id)
            lines.append(f"- Role: {role_display(role, entry.id)}")
            cache = state.client.app.cache
            if cache is not None:
                members = cache.get_members_view_for_guild(guild_id)
                if members is not None:
                    for member in [member for member in members.values() if entry.id in member.role_ids]:
                        lines.append(f"  - User: {member.mention}")
        return lines

    async def view_admins(
        state: TimeoutState,
        guild_id: int,
        ctx: arc.GatewayContext,
    ) -> None:
        lines: list[str] = []
        for admin in admins:
            lines.extend(await format_moderator_entry(state, guild_id, admin))
        msg = "\n".join(lines) if lines else "No admins configured"
        await send_pagination(
            state,
            ctx,
            f"Admins:\n{msg}",
            page_size=1000,
        )

    async def view_guild_moderators(
        state: TimeoutState,
        guild_id: int,
        ctx: arc.GatewayContext,
    ) -> None:
        lines: list[str] = []
        for moderator in guild_moderators:
            lines.extend(await format_moderator_entry(state, guild_id, moderator))
        msg = "\n".join(lines) if lines else "No guild moderators configured"
        await send_pagination(
            state,
            ctx,
            f"Guild moderators:\n{msg}",
            page_size=1000,
        )

    async def view_channel_moderators(
        state: TimeoutState,
        guild_id: int,
        ctx: arc.GatewayContext,
    ) -> None:
        channel = await fetch_channel(state, ctx.channel_id)
        channel = await fetch_parent_channel(state, channel)
        lines: list[str] = []
        for moderator in channel_moderators:
            if moderator.channel_id != channel.id:
                continue
            lines.extend(await format_moderator_entry(state, guild_id, moderator))
        msg = "\n".join(lines) if lines else f"No channel moderators configured for {channel.mention}"
        await send_pagination(
            state,
            ctx,
            f"Channel moderators in {channel.mention}:\n{msg}",
            page_size=1000,
        )

    async def view_timeout_members(
        state: TimeoutState,
        guild_id: int,
        ctx: arc.GatewayContext,
    ) -> None:
        channel = await fetch_channel(state, ctx.channel_id)
        channel = await fetch_parent_channel(state, channel)
        now = _now_utc()
        channel_prisoners = [p for p in timeout_members if p.channel_id == channel.id and not p.is_global]
        global_prisoners = [p for p in timeout_members if p.is_global]
        lines: list[str] = []
        if channel_prisoners:
            lines.append(f"Timed out users in {channel.mention}:")
            for timeout_member in channel_prisoners:
                timeleft = _ensure_utc(timeout_member.release_datetime) - now
                member = await fetch_member(state, guild_id, timeout_member.id)
                lines.append(
                    f"- {user_display(member, timeout_member.id)} `{timeleft.total_seconds() / 60:.2f} min remaining`",
                )
        else:
            lines.append(f"No timed out users in {channel.mention}")
        if global_prisoners:
            lines.append("\nTimed out users in guild:")
            for timeout_member in global_prisoners:
                timeleft = _ensure_utc(timeout_member.release_datetime) - now
                member = await fetch_member(state, guild_id, timeout_member.id)
                lines.append(
                    f"- {user_display(member, timeout_member.id)} `{timeleft.total_seconds() / 60:.2f} min remaining`",
                )
        await send_pagination(
            state,
            ctx,
            "\n".join(lines),
            page_size=2000,
            timeout=10,
        )

    async def view_all(
        state: TimeoutState,
        guild_id: int,
        ctx: arc.GatewayContext,
    ) -> None:
        lines: list[str] = []
        channel_config = _setting(SettingType.LOG_CHANNEL)
        minute_config = _setting(SettingType.MINUTE_LIMIT)
        log_channel_status = (
            mention_channel(channel_config.primary_value)
            if str(guild_id) == channel_config.secondary_value
            else "unconfigured"
        )
        lines.append(f"Log channel: {log_channel_status}")
        lines.append(f"Timeout limit: `{minute_config.primary_value} minutes`")
        lines.append("\nAdmins:")
        for admin in admins:
            lines.extend(await format_moderator_entry(state, guild_id, admin))
        lines.append("\nGuild moderators:")
        for moderator in guild_moderators:
            lines.extend(await format_moderator_entry(state, guild_id, moderator))
        channel_moderators_by_channel: dict[int, list[ChannelModerator]] = {}
        for cm in channel_moderators:
            channel_moderators_by_channel.setdefault(cm.channel_id, []).append(cm)
        for channel_id, moderators in channel_moderators_by_channel.items():
            channel = state.client.app.cache.get_guild_channel(channel_id) if state.client.app.cache else None
            if channel is None:
                continue
            lines.append(f"\nModerating {channel.mention}:")
            for moderator in moderators:
                lines.extend(await format_moderator_entry(state, guild_id, moderator))
        prisoners_by_channel: dict[int, list[TimeoutMember]] = {}
        for timeout_member in timeout_members:
            prisoners_by_channel.setdefault(timeout_member.channel_id, []).append(
                timeout_member,
            )
        now = _now_utc()
        for channel_id, channel_prisoners in prisoners_by_channel.items():
            channel = state.client.app.cache.get_guild_channel(channel_id) if state.client.app.cache else None
            if channel is None:
                continue
            lines.append(f"\nFound timed out users in {channel.mention}:")
            for timeout_member in channel_prisoners:
                timeleft = _ensure_utc(timeout_member.release_datetime) - now
                member = await fetch_member(state, guild_id, timeout_member.id)
                lines.append(
                    f"- {user_display(member, timeout_member.id)} `{timeleft.total_seconds() / 60:.2f} min remaining`",
                )
        await send_pagination(
            state,
            ctx,
            f"Timeout Configuration:\n\n{''.join(lines)}",
            page_size=1000,
        )

    state = get_state()
    guild_id = ctx.guild_id
    if guild_id is None:
        await reply_err(state.client.app, ctx, "Failed to provide guild context.")
        return

    handlers: dict[
        str,
        Callable[[TimeoutState, int, arc.GatewayContext], Awaitable[None]],
    ] = {
        "admin": view_admins,
        "guild_moderator": view_guild_moderators,
        "channel_moderator": view_channel_moderators,
        "timeout_members": view_timeout_members,
        "all": view_all,
    }

    handler = handlers.get(view_type)
    if handler is not None:
        await handler(state, guild_id, ctx)


@group_channel.include
@arc.with_hook(check_channel_moderator)
@arc.slash_subcommand(
    "timeout",
    description="Timeout a member in the channel",
)
async def cmd_channel_timeout(
    ctx: arc.GatewayContext,
    user: arc.Option[
        hikari.User,
        arc.UserParams(
            name="user",
            description="The user to timeout",
        ),
    ],
    minutes: arc.Option[
        int,
        arc.IntParams(
            name="minutes",
            description="The minutes to timeout",
        ),
    ],
) -> None:
    state = get_state()
    if ctx.guild_id is None:
        await reply_err(state.client.app, ctx, "Failed to provide guild context.")
        return
    await defer(ctx)
    channel = await fetch_channel(state, ctx.channel_id)
    channel = await fetch_parent_channel(state, channel)
    member = await fetch_member(state, int(ctx.guild_id), int(user.id))
    if member is None:
        await reply_err(state.client.app, ctx, "Failed to locate member.")
        return
    await state.timeout_channel(member, minutes, channel, ctx=ctx)


@group_guild.include
@arc.with_hook(check_guild_moderator)
@arc.slash_subcommand(
    "timeout",
    description="Timeout a member in the guild",
)
async def cmd_guild_timeout(
    ctx: arc.GatewayContext,
    user: arc.Option[
        hikari.User,
        arc.UserParams(
            name="user",
            description="The user to timeout",
        ),
    ],
    minutes: arc.Option[
        int,
        arc.IntParams(
            name="minutes",
            description="The minutes to timeout",
        ),
    ],
) -> None:
    state = get_state()
    if ctx.guild_id is None:
        await reply_err(state.client.app, ctx, "Failed to provide guild context.")
        return
    await defer(ctx)
    member = await fetch_member(state, int(ctx.guild_id), int(user.id))
    if member is None:
        await reply_err(state.client.app, ctx, "Failed to locate member.")
        return
    await state.timeout_guild(member, minutes, ctx=ctx)


async def autocomplete_guild_release(
    ctx: arc.AutocompleteData[arc.GatewayClient, str],
) -> Mapping[str, str]:
    guild_id = ctx.interaction.guild_id
    if guild_id is None:
        return {}
    state = get_state()
    focused = ctx.focused_option
    option_input = str(focused.value) if focused and focused.value else ""
    global_prisoners = [p for p in timeout_members if p.is_global]
    if not global_prisoners:
        return {}
    members = await asyncio.gather(
        *(fetch_member(state, int(guild_id), p.id) for p in global_prisoners),
        return_exceptions=True,
    )
    valid_members = [m for m in members if isinstance(m, hikari.Member)]
    filtered = [
        m for m in valid_members if option_input in (name_display(m) or m.username) or option_input in m.username
    ]
    return {(name_display(m) or m.username): str(m.id) for m in filtered[:25]}


@group_guild.include
@arc.with_hook(check_guild_moderator)
@arc.slash_subcommand(
    "release",
    description="Release a member in the guild",
)
async def cmd_guild_release(
    ctx: arc.GatewayContext,
    user: arc.Option[
        str,
        arc.StrParams(
            name="user",
            description="The user to release",
            autocomplete_with=autocomplete_guild_release,
        ),
    ],
) -> None:
    state = get_state()
    guild_id = ctx.guild_id
    if guild_id is None:
        await reply_err(state.client.app, ctx, "Failed to provide guild context.")
        return
    await defer(ctx)
    try:
        user_id = int(user)
    except ValueError:
        await reply_err(state.client.app, ctx, "Failed to parse user ID format.")
        return
    member = await fetch_member(state, int(guild_id), user_id)
    if member is None:
        await reply_err(state.client.app, ctx, "Failed to locate member.")
        return
    global_prisoners = next(
        (p for p in timeout_members if p.id == user_id and p.is_global),
        None,
    )
    if global_prisoners is None:
        await reply_ok(
            state.client.app,
            ctx,
            f"{member.mention} was not timed out in the guild.",
        )
        return
    await state.release(timeout_member=global_prisoners, ctx=ctx)


async def autocomplete_channel_release(
    ctx: arc.AutocompleteData[arc.GatewayClient, str],
) -> Mapping[str, str]:
    if ctx.interaction.guild_id is None or ctx.interaction.channel_id is None:
        return {}
    state = get_state()
    option = ctx.focused_option
    option_input = str(option.value) if option and option.value else ""
    channel = await fetch_channel(state, ctx.interaction.channel_id)
    channel = await fetch_parent_channel(state, channel)
    options_user = await asyncio.gather(
        *(
            fetch_member(state, int(ctx.interaction.guild_id), i.id)
            for i in timeout_members
            if i.channel_id == channel.id
        ),
        return_exceptions=True,
    )
    members: list[hikari.Member] = [m for m in options_user if isinstance(m, hikari.Member)]
    return {
        name_display(m): str(m.id)
        for m in members[:25]
        if option_input in name_display(m) or option_input in str(m.username)
    }


@group_channel.include
@arc.with_hook(check_channel_moderator)
@arc.slash_subcommand(
    "release",
    description="Release a member in the channel",
)
async def cmd_channel_release(
    ctx: arc.GatewayContext,
    user: arc.Option[
        str,
        arc.StrParams(
            name="user",
            description="The user to release",
            autocomplete_with=autocomplete_channel_release,
        ),
    ],
) -> None:
    state = get_state()
    if ctx.guild_id is None:
        await reply_err(state.client.app, ctx, "Failed to provide guild context.")
        return
    await defer(ctx)
    try:
        user_id = int(user)
    except ValueError:
        await reply_err(state.client.app, ctx, "Failed to parse input format.")
        return
    channel = await fetch_channel(state, ctx.channel_id)
    channel = await fetch_parent_channel(state, channel)
    member = await fetch_member(state, int(ctx.guild_id), user_id)
    if member is None:
        await reply_err(state.client.app, ctx, "Failed to locate member.")
        return
    timeout, timeout_member = state.check_timeout_member(member, 1, channel)
    if not timeout:
        await reply_ok(
            state.client.app,
            ctx,
            f"{member.mention} was not timed out in the channel.",
        )
        return
    await state.release(timeout_member=timeout_member, ctx=ctx)


@plugin.listen(hikari.StartedEvent)
async def on_timeout_started(_: hikari.StartedEvent) -> None:
    state = get_state()
    await state.async_init()


@arc.loader
def load(client: arc.GatewayClient) -> None:
    global _state
    if _state is not None:
        return

    state = TimeoutState(client)
    _state = state
    client.add_plugin(plugin)


@arc.unloader
def unload(client: arc.GatewayClient) -> None:
    global _state
    client.remove_plugin(plugin)
    if _state is not None:
        _state.drop()
        _state = None
