from __future__ import annotations

import collections
import time
from dataclasses import dataclass, field

from snf_wire import EnemyKind, EventTag, ProjectileRemovalReason, RoomPhase


@dataclass
class Entity:
    id: int
    x: float = 0.0
    y: float = 0.0
    hp: int = 100
    max_hp: int = 100
    prev_x: float = 0.0
    prev_y: float = 0.0
    updated_at: float = field(default_factory=time.monotonic)
    alive: bool = True
    kind: EnemyKind = EnemyKind.Minion

    def set_pos(self, new_x: float, new_y: float, now: float | None = None) -> None:
        current_now = time.monotonic() if now is None else now
        self.prev_x = self.x
        self.prev_y = self.y
        self.x = float(new_x)
        self.y = float(new_y)
        self.updated_at = current_now

    def interpolated_pos(self, now: float | None = None, window: float = 0.1) -> tuple[float, float]:
        current_now = time.monotonic() if now is None else now
        if window <= 0.0:
            return self.x, self.y
        elapsed = current_now - self.updated_at
        alpha = max(0.0, min(1.0, elapsed / window))
        ix = self.prev_x + (self.x - self.prev_x) * alpha
        iy = self.prev_y + (self.y - self.prev_y) * alpha
        return ix, iy


@dataclass
class ProjectileState:
    id: int
    owner: int
    skill_id: int
    target: int
    x: float
    y: float
    prev_x: float
    prev_y: float
    updated_at: float = field(default_factory=time.monotonic)

    def set_pos(self, new_x: float, new_y: float, now: float | None = None) -> None:
        current_now = time.monotonic() if now is None else now
        self.prev_x = self.x
        self.prev_y = self.y
        self.x = float(new_x)
        self.y = float(new_y)
        self.updated_at = current_now

    def interpolated_pos(self, now: float | None = None, window: float = 0.1) -> tuple[float, float]:
        current_now = time.monotonic() if now is None else now
        if window <= 0.0:
            return self.x, self.y
        alpha = max(0.0, min(1.0, (current_now - self.updated_at) / window))
        return (
            self.prev_x + (self.x - self.prev_x) * alpha,
            self.prev_y + (self.y - self.prev_y) * alpha,
        )


class World:
    def __init__(self, arena_w: int = 100, arena_h: int = 100) -> None:
        self.mode: str = "zone"

        self.zone_id: int = 1
        self.zone_x: int = 0
        self.zone_y: int = 0
        self.zone_prev_x: float = 0.0
        self.zone_prev_y: float = 0.0
        self.zone_pos_updated_at: float = time.monotonic()
        self.zone_epoch: int = 0
        self.zone_visible_players: list[int] = []
        self.zone_peers: dict[int, Entity] = {}

        self.arena_w: int = arena_w
        self.arena_h: int = arena_h
        self.phase: RoomPhase = RoomPhase.Waiting
        self.digest_seq: int = 0
        self.players: dict[int, Entity] = {}
        self.enemies: dict[int, Entity] = {}
        self.projectiles: dict[int, ProjectileState] = {}
        self.boss_id: int | None = None
        self.log: collections.deque[str] = collections.deque(maxlen=8)
        self.battle_start_time: float | None = None
        self.cleared_exp: int | None = None
        self.failure_reason: str | None = None
        self.boss_final_hp: int | None = None
        self.returned_zone: tuple[int, int, int] | None = None

    def add_log(self, message: str) -> None:
        self.log.append(message)

    def apply_zone_state(
        self,
        zone_id: int,
        epoch: int,
        x: int,
        y: int,
        visible_players: list[int],
        now: float | None = None,
    ) -> None:
        import math

        current_now = time.monotonic() if now is None else now
        self.zone_id = zone_id
        self.zone_epoch = epoch
        self.zone_prev_x = float(self.zone_x)
        self.zone_prev_y = float(self.zone_y)
        self.zone_x = x
        self.zone_y = y
        self.zone_pos_updated_at = current_now
        self.zone_visible_players = visible_players

        current_peers = set(visible_players)
        for pid in list(self.zone_peers.keys()):
            if pid not in current_peers:
                del self.zone_peers[pid]
                self.add_log(f"Player #{pid} left the area")

        for pid in visible_players:
            if pid not in self.zone_peers:
                angle = (pid * 137.5) * math.pi / 180.0
                dist = 28.0 + (pid % 4) * 16.0
                peer_x = math.cos(angle) * dist
                peer_y = math.sin(angle) * dist
                peer = Entity(
                    id=pid,
                    x=peer_x,
                    y=peer_y,
                    prev_x=peer_x,
                    prev_y=peer_y,
                    updated_at=current_now,
                )
                self.zone_peers[pid] = peer
                self.add_log(f"Player #{pid} appeared in Zone #{zone_id}")

    def get_zone_peer_pos(self, peer_id: int, now: float | None = None) -> tuple[float, float]:
        import math

        current_now = time.monotonic() if now is None else now
        peer = self.zone_peers.get(peer_id)
        if not peer:
            angle = (peer_id * 137.5) * math.pi / 180.0
            dist = 28.0 + (peer_id % 4) * 16.0
            return math.cos(angle) * dist, math.sin(angle) * dist

        idle_x = peer.x + math.sin(current_now * 1.2 + peer_id * 1.7) * 3.5
        idle_y = peer.y + math.cos(current_now * 0.9 + peer_id * 2.3) * 3.5
        return idle_x, idle_y

    def enter_room_mode(self) -> None:
        self.mode = "room"
        self.players.clear()
        self.enemies.clear()
        self.projectiles.clear()
        self.boss_id = None
        self.digest_seq = 0
        self.phase = RoomPhase.Waiting
        self.battle_start_time = None
        self.cleared_exp = None
        self.failure_reason = None
        self.boss_final_hp = None
        self.add_log("Entered Battle Room")

    def return_to_zone_mode(self, zone_id: int, x: int, y: int) -> None:
        self.mode = "zone"
        self.zone_id = zone_id
        self.zone_prev_x = float(x)
        self.zone_prev_y = float(y)
        self.zone_x = x
        self.zone_y = y
        self.zone_pos_updated_at = time.monotonic()
        self.players.clear()
        self.enemies.clear()
        self.projectiles.clear()
        self.boss_id = None
        self.add_log(f"Returned to Zone {zone_id} at ({x}, {y})")

    def apply_digest(
        self,
        seq: int,
        phase: RoomPhase,
        events: list[tuple[EventTag, dict]],
        now: float | None = None,
    ) -> None:
        current_now = time.monotonic() if now is None else now
        if self.digest_seq != 0 and seq != self.digest_seq + 1:
            self.add_log(f"[WARN] Digest sequence gap: expected {self.digest_seq + 1}, got {seq}")
        self.digest_seq = seq
        self.phase = phase

        for tag, ev in events:
            if tag == EventTag.ArenaStarted:
                self.arena_w = ev["width"]
                self.arena_h = ev["height"]
                self.digest_seq = seq
                if self.battle_start_time is None:
                    self.battle_start_time = current_now
                self.add_log(f"Arena started: {self.arena_w}x{self.arena_h}")

            elif tag == EventTag.EnemySpawned:
                eid = ev["id"]
                kind = EnemyKind(ev["kind"])
                hp = ev["hp"]
                enemy = Entity(id=eid, hp=hp, max_hp=hp, kind=kind, updated_at=current_now)
                self.enemies[eid] = enemy
                if kind == EnemyKind.Boss:
                    self.boss_id = eid
                    self.add_log(f"BOSS spawned! ID={eid} HP={hp}")
                else:
                    self.add_log(f"Spawned minion #{eid} HP={hp}")

            elif tag == EventTag.EnemyPositioned:
                eid = ev["enemy"]
                if eid in self.enemies:
                    self.enemies[eid].set_pos(ev["x"], ev["y"], now=current_now)

            elif tag == EventTag.EnemyDamaged:
                eid = ev["target"]
                if eid in self.enemies:
                    self.enemies[eid].hp = ev["hp"]
                self.add_log(f"Enemy #{eid} -{ev['amount']} dmg from P{ev['actor']} (HP: {ev['hp']})")

            elif tag == EventTag.EnemyDied:
                eid = ev["id"]
                if eid in self.enemies:
                    del self.enemies[eid]
                if self.boss_id == eid:
                    self.boss_id = None
                self.add_log(f"Enemy #{eid} died")

            elif tag == EventTag.ProjectileSpawned:
                projectile = ProjectileState(
                    id=ev["projectile"],
                    owner=ev["owner"],
                    skill_id=ev["skill_id"],
                    target=ev["target"],
                    x=float(ev["x"]),
                    y=float(ev["y"]),
                    prev_x=float(ev["x"]),
                    prev_y=float(ev["y"]),
                    updated_at=current_now,
                )
                self.projectiles[projectile.id] = projectile

            elif tag == EventTag.ProjectileMoved:
                projectile = self.projectiles.get(ev["projectile"])
                if projectile:
                    projectile.set_pos(ev["x"], ev["y"], now=current_now)

            elif tag == EventTag.ProjectileRemoved:
                projectile_id = ev["projectile"]
                self.projectiles.pop(projectile_id, None)
                reason = ProjectileRemovalReason(ev["reason"])
                self.add_log(f"Projectile #{projectile_id} removed ({reason.name})")

            elif tag == EventTag.ParticipantSpawned:
                pid = ev["player"]
                p = Entity(
                    id=pid,
                    x=float(ev["x"]),
                    y=float(ev["y"]),
                    prev_x=float(ev["x"]),
                    prev_y=float(ev["y"]),
                    hp=ev["hp"],
                    max_hp=ev["hp"],
                    updated_at=current_now,
                    alive=True,
                )
                self.players[pid] = p
                self.add_log(f"Player {pid} spawned at ({ev['x']}, {ev['y']}) HP={ev['hp']}")

            elif tag == EventTag.ParticipantMoved:
                pid = ev["player"]
                if pid in self.players:
                    self.players[pid].set_pos(ev["x"], ev["y"], now=current_now)

            elif tag == EventTag.ParticipantDamaged:
                pid = ev["target"]
                if pid in self.players:
                    self.players[pid].hp = ev["hp"]
                self.add_log(f"Player {pid} took {ev['amount']} dmg from E#{ev['attacker']} (HP: {ev['hp']})")

            elif tag == EventTag.ParticipantDied:
                pid = ev["player"]
                if pid in self.players:
                    self.players[pid].alive = False
                    self.players[pid].hp = 0
                self.add_log(f"Player {pid} was defeated")

            elif tag == EventTag.ParticipantLeft:
                pid = ev["player"]
                if pid in self.players:
                    del self.players[pid]
                self.add_log(f"Player {pid} left the room")

            elif tag == EventTag.SkillWhiffed:
                self.add_log(f"Player {ev['actor']} whiffed skill #{ev['skill_id']}")

        if phase in (RoomPhase.Cleared, RoomPhase.Failed):
            self.projectiles.clear()
