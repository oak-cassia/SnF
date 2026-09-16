from __future__ import annotations

import struct
from dataclasses import dataclass
from enum import IntEnum
from typing import Generator


class MessageType(IntEnum):
    Ping = 1
    Pong = 2
    Authenticate = 3
    Authenticated = 4
    EnterZone = 5
    ZoneEntered = 6
    Move = 7
    Moved = 8
    LeaveZone = 9
    ZoneLeft = 10
    Purchase = 11
    PurchaseResult = 12
    RoomJoin = 17
    RoomJoined = 18
    BattleStart = 19
    BattleStarted = 20
    BattleCleared = 21
    RoomLeave = 22
    RoomLeft = 23
    ReturnedToZone = 24
    UseSkill = 25
    SkillApplied = 26
    BattleFailed = 27
    BattleDigest = 28
    SkillAcknowledged = 29
    SetMoveIntent = 30
    MoveAcknowledged = 31
    EquipSkill = 32
    EquipSkillResult = 33


class RoomStatus(IntEnum):
    Applied = 0
    AlreadyJoined = 1
    RoomFull = 2
    WrongPhase = 3
    NotJoined = 4
    EntryFailed = 5
    DuplicateRequest = 6
    SkillOnCooldown = 7
    UnknownSkill = 8
    BattleExpired = 9
    ParticipantDead = 10
    RuntimeOverloaded = 11
    ProjectileCapacityExceeded = 12
    SkillNotEquipped = 13


class PurchaseStatus(IntEnum):
    Committed = 0
    InsufficientFunds = 1
    ProductNotFound = 2
    InventoryCapacityExceeded = 3
    IdempotencyConflict = 4
    IdempotencyCapacityExceeded = 5
    Unavailable = 6
    AlreadyOwned = 7


class EquipSkillStatus(IntEnum):
    Equipped = 0
    AlreadyEquipped = 1
    SkillNotOwned = 2
    UnknownSkill = 3


class ZoneCommandStatus(IntEnum):
    Applied = 0
    AlreadyPresent = 1
    PlayerMissing = 2
    StaleRoute = 3
    TransitionInProgress = 4
    TransferFailed = 5
    InRoom = 6


class RoomPhase(IntEnum):
    Waiting = 0
    Running = 1
    Cleared = 2
    Failed = 3


class BattleFailureReason(IntEnum):
    Deadline = 0
    ParticipantsDefeated = 1


class EnemyKind(IntEnum):
    Minion = 0
    Boss = 1


class Direction(IntEnum):
    Stop = 0
    North = 1
    NorthEast = 2
    East = 3
    SouthEast = 4
    South = 5
    SouthWest = 6
    West = 7
    NorthWest = 8


class EventTag(IntEnum):
    EnemySpawned = 0
    EnemyDamaged = 1
    EnemyDied = 2
    SkillWhiffed = 3
    ArenaStarted = 4
    ParticipantSpawned = 5
    ParticipantMoved = 6
    EnemyPositioned = 7
    ParticipantDamaged = 8
    ParticipantDied = 9
    ParticipantLeft = 10
    ProjectileSpawned = 11
    ProjectileMoved = 12
    ProjectileRemoved = 13


class ProjectileRemovalReason(IntEnum):
    Hit = 0
    TargetLost = 1
    Expired = 2


_EVENT_SPECS: dict[EventTag, tuple[str, tuple[str, ...]]] = {
    EventTag.EnemySpawned: (">IBQ", ("id", "kind", "hp")),
    EventTag.EnemyDamaged: (">IQIQQ", ("target", "actor", "skill_id", "amount", "hp")),
    EventTag.EnemyDied: (">I", ("id",)),
    EventTag.SkillWhiffed: (">QI", ("actor", "skill_id")),
    EventTag.ArenaStarted: (">II", ("width", "height")),
    EventTag.ParticipantSpawned: (">QIIQ", ("player", "x", "y", "hp")),
    EventTag.ParticipantMoved: (">QII", ("player", "x", "y")),
    EventTag.EnemyPositioned: (">III", ("enemy", "x", "y")),
    EventTag.ParticipantDamaged: (">QIQQ", ("target", "attacker", "amount", "hp")),
    EventTag.ParticipantDied: (">Q", ("player",)),
    EventTag.ParticipantLeft: (">Q", ("player",)),
    EventTag.ProjectileSpawned: (">IQIIII", ("projectile", "owner", "skill_id", "target", "x", "y")),
    EventTag.ProjectileMoved: (">III", ("projectile", "x", "y")),
    EventTag.ProjectileRemoved: (">IB", ("projectile", "reason")),
}

MIN_BODY_SIZE = 6
MAX_BODY_SIZE = 64 * 1024
UNSOLICITED_REQUEST_ID = 0
SLASH_SKILL_ID = 1
ARCANE_BOLT_SKILL_ID = 2
ARCANE_BOLT_PRODUCT_ID = 2


@dataclass(frozen=True)
class Frame:
    type: MessageType
    request_id: int
    payload: bytes = b""


def encode(msg_type: int | MessageType, request_id: int, payload: bytes = b"") -> bytes:
    body_len = MIN_BODY_SIZE + len(payload)
    if body_len > MAX_BODY_SIZE:
        raise ValueError(f"Body length {body_len} exceeds maximum allowed size ({MAX_BODY_SIZE})")
    return struct.pack(">IHI", body_len, int(msg_type), request_id) + payload


class FrameDecoder:
    def __init__(self) -> None:
        self._buf = bytearray()
        self._offset = 0

    def feed(self, data: bytes) -> None:
        if self._offset > 0:
            del self._buf[: self._offset]
            self._offset = 0
        self._buf.extend(data)

    def frames(self) -> Generator[Frame, None, None]:
        while True:
            remaining = len(self._buf) - self._offset
            if remaining < 4:
                break
            body_len = struct.unpack_from(">I", self._buf, self._offset)[0]
            if body_len < MIN_BODY_SIZE or body_len > MAX_BODY_SIZE:
                raise ValueError(f"Invalid frame body length: {body_len}")
            total_len = 4 + body_len
            if remaining < total_len:
                break

            raw_type, request_id = struct.unpack_from(">HI", self._buf, self._offset + 4)
            payload_start = self._offset + 10
            payload_end = self._offset + total_len
            payload = bytes(self._buf[payload_start:payload_end])
            self._offset += total_len

            try:
                msg_type = MessageType(raw_type)
            except ValueError:
                raise ValueError(f"Unknown message type: {raw_type}")

            yield Frame(type=msg_type, request_id=request_id, payload=payload)

        if self._offset > 0:
            del self._buf[: self._offset]
            self._offset = 0



def authenticate(player_id: int) -> bytes:
    if player_id == 0:
        raise ValueError("player_id must be non-zero")
    return struct.pack(">Q", player_id)


def purchase(idempotency_key: int, product_id: int) -> bytes:
    if idempotency_key == 0:
        raise ValueError("idempotency_key must be non-zero")
    if product_id == 0:
        raise ValueError("product_id must be non-zero")
    return struct.pack(">QI", idempotency_key, product_id)


def equip_skill(skill_id: int) -> bytes:
    if skill_id == 0:
        raise ValueError("skill_id must be non-zero")
    return struct.pack(">I", skill_id)


def enter_zone(zone_id: int, x: int = 0, y: int = 0) -> bytes:
    return struct.pack(">Qii", zone_id, x, y)


def room_join(room_id: int) -> bytes:
    if room_id == 0:
        raise ValueError("room_id must be non-zero")
    return struct.pack(">Q", room_id)


def battle_start(room_id: int) -> bytes:
    if room_id == 0:
        raise ValueError("room_id must be non-zero")
    return struct.pack(">Q", room_id)


def set_move_intent(room_id: int, direction: int | Direction, seq: int) -> bytes:
    if room_id == 0:
        raise ValueError("room_id must be non-zero")
    if seq == 0:
        raise ValueError("sequence must be non-zero")
    dir_val = int(direction)
    if dir_val < 0 or dir_val > int(Direction.NorthWest):
        raise ValueError(f"Invalid move direction: {dir_val}")
    return struct.pack(">QBQ", room_id, dir_val, seq)


def use_skill(room_id: int, skill_id: int, seq: int) -> bytes:
    if room_id == 0:
        raise ValueError("room_id must be non-zero")
    if skill_id == 0:
        raise ValueError("skill_id must be non-zero")
    if seq == 0:
        raise ValueError("sequence must be non-zero")
    return struct.pack(">QIQ", room_id, skill_id, seq)


def move_in_zone(x: int, y: int) -> bytes:
    return struct.pack(">ii", x, y)


def leave_zone() -> bytes:
    return b""


def room_leave() -> bytes:
    return b""



def parse_authenticated(payload: bytes) -> int:
    if len(payload) != 8:
        raise ValueError(f"Authenticated payload must be 8 bytes, got {len(payload)}")
    return struct.unpack(">Q", payload)[0]


def parse_purchase_result(payload: bytes) -> dict:
    if len(payload) != 30:
        raise ValueError(f"PurchaseResult payload must be 30 bytes, got {len(payload)}")
    status, replayed, idempotency_key, product_id, currency_balance, purchased_item_count = struct.unpack(
        ">BBQIQQ", payload
    )
    return {
        "status": PurchaseStatus(status),
        "replayed": bool(replayed),
        "idempotency_key": idempotency_key,
        "product_id": product_id,
        "currency_balance": currency_balance,
        "purchased_item_count": purchased_item_count,
    }


def parse_equip_skill_result(payload: bytes) -> tuple[EquipSkillStatus, int]:
    if len(payload) != 5:
        raise ValueError(f"EquipSkillResult payload must be 5 bytes, got {len(payload)}")
    status, equipped_skill_id = struct.unpack(">BI", payload)
    return EquipSkillStatus(status), equipped_skill_id


def parse_zone_entered(payload: bytes) -> dict:
    if len(payload) < 27:
        raise ValueError(f"ZoneEntered payload too short: {len(payload)} < 27")
    status, zone_id, epoch, x, y, visible_count = struct.unpack_from(">BQQiiH", payload, 0)
    expected_len = 27 + visible_count * 8
    if len(payload) != expected_len:
        raise ValueError(f"ZoneEntered payload size mismatch: expected {expected_len}, got {len(payload)}")
    visible_players = list(struct.unpack_from(f">{visible_count}Q", payload, 27)) if visible_count > 0 else []
    return {
        "status": status,
        "zone_id": zone_id,
        "epoch": epoch,
        "x": x,
        "y": y,
        "visible_players": visible_players,
    }


def parse_moved(payload: bytes) -> dict:
    return parse_zone_entered(payload)


def parse_room_reply(payload: bytes) -> tuple[RoomStatus, RoomPhase, int]:
    if len(payload) != 10:
        raise ValueError(f"Room reply payload must be 10 bytes, got {len(payload)}")
    status_raw, phase_raw, room_id = struct.unpack(">BBQ", payload)
    return RoomStatus(status_raw), RoomPhase(phase_raw), room_id


def parse_ack(payload: bytes) -> tuple[RoomStatus, RoomPhase]:
    if len(payload) != 2:
        raise ValueError(f"Ack payload must be 2 bytes, got {len(payload)}")
    status_raw, phase_raw = struct.unpack(">BB", payload)
    return RoomStatus(status_raw), RoomPhase(phase_raw)


def parse_cleared(payload: bytes) -> int:
    if len(payload) != 8:
        raise ValueError(f"BattleCleared payload must be 8 bytes, got {len(payload)}")
    return struct.unpack(">Q", payload)[0]


def parse_failed(payload: bytes) -> tuple[int, bool, BattleFailureReason]:
    if len(payload) != 10:
        raise ValueError(f"BattleFailed payload must be 10 bytes, got {len(payload)}")
    boss_hp, boss_spawned, reason = struct.unpack(">QBB", payload)
    return boss_hp, bool(boss_spawned), BattleFailureReason(reason)


def parse_returned(payload: bytes) -> tuple[int, int, int]:
    if len(payload) != 16:
        raise ValueError(f"ReturnedToZone payload must be 16 bytes, got {len(payload)}")
    zone_id, x, y = struct.unpack(">Qii", payload)
    return zone_id, x, y


def parse_digest(payload: bytes) -> tuple[int, RoomPhase, list[tuple[EventTag, dict]]]:
    if len(payload) < 11:
        raise ValueError(f"BattleDigest payload too short: {len(payload)} < 11")
    seq, phase_raw, count = struct.unpack_from(">QBH", payload, 0)
    phase = RoomPhase(phase_raw)
    events: list[tuple[EventTag, dict]] = []
    offset = 11

    for _ in range(count):
        if offset >= len(payload):
            raise ValueError(f"Digest truncated while reading event tag at offset {offset}")
        tag_val = payload[offset]
        offset += 1
        try:
            tag = EventTag(tag_val)
        except ValueError:
            raise ValueError(f"Unknown event tag: {tag_val}")

        if tag not in _EVENT_SPECS:
            raise ValueError(f"No spec for event tag: {tag.name}")

        fmt, fields = _EVENT_SPECS[tag]
        size = struct.calcsize(fmt)
        if offset + size > len(payload):
            raise ValueError(f"Digest truncated while reading {tag.name}: need {size} bytes at {offset}, total {len(payload)}")

        vals = struct.unpack_from(fmt, payload, offset)
        offset += size
        events.append((tag, dict(zip(fields, vals))))

    if offset != len(payload):
        raise ValueError(f"Digest has {len(payload) - offset} unparsed trailing bytes")

    return seq, phase, events
