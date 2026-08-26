from __future__ import annotations

import struct
import unittest

import snf_wire
from snf_bot import BotPlayer
from snf_play import join_party_room, zone_render_position_after_response
from snf_wire import (
    BattleFailureReason,
    Direction,
    EnemyKind,
    EventTag,
    FrameDecoder,
    MessageType,
    ProjectileRemovalReason,
    RoomPhase,
    RoomStatus,
    ZoneCommandStatus,
)
from snf_world import World
from snf_session import Session


class TestWireProtocol(unittest.TestCase):
    def test_encode_and_decode_frames(self) -> None:
        payload = b"Hello SnF"
        encoded = snf_wire.encode(MessageType.Authenticate, 42, payload)

        decoder = FrameDecoder()
        decoder.feed(encoded)
        frames = list(decoder.frames())
        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0].type, MessageType.Authenticate)
        self.assertEqual(frames[0].request_id, 42)
        self.assertEqual(frames[0].payload, payload)

    def test_partial_feed_frame_decoder(self) -> None:
        payload = b"1234567890"
        encoded = snf_wire.encode(MessageType.Ping, 101, payload)

        decoder = FrameDecoder()
        decoder.feed(encoded[:4])
        self.assertEqual(list(decoder.frames()), [])

        decoder.feed(encoded[4:8])
        self.assertEqual(list(decoder.frames()), [])

        decoder.feed(encoded[8:])
        frames = list(decoder.frames())
        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0].type, MessageType.Ping)
        self.assertEqual(frames[0].request_id, 101)
        self.assertEqual(frames[0].payload, payload)

    def test_request_builders(self) -> None:
        auth = snf_wire.authenticate(12345)
        self.assertEqual(len(auth), 8)
        self.assertEqual(struct.unpack(">Q", auth)[0], 12345)

        zone = snf_wire.enter_zone(1, -100, 200)
        self.assertEqual(len(zone), 16)
        zid, zx, zy = struct.unpack(">Qii", zone)
        self.assertEqual(zid, 1)
        self.assertEqual(zx, -100)
        self.assertEqual(zy, 200)

        rjoin = snf_wire.room_join(7)
        self.assertEqual(len(rjoin), 8)
        bstart = snf_wire.battle_start(7)
        self.assertEqual(len(bstart), 8)

        move = snf_wire.set_move_intent(7, Direction.NorthEast, 5)
        self.assertEqual(len(move), 17)
        rm, d, sq = struct.unpack(">QBQ", move)
        self.assertEqual(rm, 7)
        self.assertEqual(d, int(Direction.NorthEast))
        self.assertEqual(sq, 5)

        skill = snf_wire.use_skill(7, 1, 10)
        self.assertEqual(len(skill), 20)
        rm, sk, sq = struct.unpack(">QIQ", skill)
        self.assertEqual(rm, 7)
        self.assertEqual(sk, 1)
        self.assertEqual(sq, 10)

    def test_response_parsers(self) -> None:
        self.assertEqual(snf_wire.parse_authenticated(struct.pack(">Q", 99)), 99)

        status, phase, room = snf_wire.parse_room_reply(
            struct.pack(">BBQ", int(RoomStatus.Applied), int(RoomPhase.Running), 77)
        )
        self.assertEqual(status, RoomStatus.Applied)
        self.assertEqual(phase, RoomPhase.Running)
        self.assertEqual(room, 77)

        status, phase = snf_wire.parse_ack(
            struct.pack(">BB", int(RoomStatus.Applied), int(RoomPhase.Running))
        )
        self.assertEqual(status, RoomStatus.Applied)
        self.assertEqual(phase, RoomPhase.Running)

        status, phase = snf_wire.parse_ack(
            struct.pack(">BB", int(RoomStatus.ProjectileCapacityExceeded), int(RoomPhase.Running))
        )
        self.assertEqual(status, RoomStatus.ProjectileCapacityExceeded)
        self.assertEqual(phase, RoomPhase.Running)

        self.assertEqual(snf_wire.parse_cleared(struct.pack(">Q", 500)), 500)

        hp, spawned, reason = snf_wire.parse_failed(
            struct.pack(">QBB", 250, 1, int(BattleFailureReason.ParticipantsDefeated))
        )
        self.assertEqual(hp, 250)
        self.assertTrue(spawned)
        self.assertEqual(reason, BattleFailureReason.ParticipantsDefeated)

        rz, rx, ry = snf_wire.parse_returned(struct.pack(">Qii", 1, 10, -20))
        self.assertEqual(rz, 1)
        self.assertEqual(rx, 10)
        self.assertEqual(ry, -20)

    def test_parse_digest(self) -> None:
        buf = bytearray()
        buf.extend(struct.pack(">QBH", 1, int(RoomPhase.Running), 6))

        buf.append(int(EventTag.ArenaStarted))
        buf.extend(struct.pack(">II", 100, 100))

        buf.append(int(EventTag.EnemySpawned))
        buf.extend(struct.pack(">IBQ", 10, int(EnemyKind.Minion), 50))

        buf.append(int(EventTag.ParticipantSpawned))
        buf.extend(struct.pack(">QIIQ", 1, 50, 50, 100))

        buf.append(int(EventTag.ProjectileSpawned))
        buf.extend(struct.pack(">IQIIII", 7, 1, 2, 10, 50, 50))

        buf.append(int(EventTag.ProjectileMoved))
        buf.extend(struct.pack(">III", 7, 50, 46))

        buf.append(int(EventTag.ProjectileRemoved))
        buf.extend(struct.pack(">IB", 7, int(ProjectileRemovalReason.Hit)))

        seq, phase, events = snf_wire.parse_digest(bytes(buf))
        self.assertEqual(seq, 1)
        self.assertEqual(phase, RoomPhase.Running)
        self.assertEqual(len(events), 6)

        self.assertEqual(events[0][0], EventTag.ArenaStarted)
        self.assertEqual(events[0][1]["width"], 100)
        self.assertEqual(events[0][1]["height"], 100)

        self.assertEqual(events[1][0], EventTag.EnemySpawned)
        self.assertEqual(events[1][1]["id"], 10)
        self.assertEqual(events[1][1]["kind"], int(EnemyKind.Minion))
        self.assertEqual(events[1][1]["hp"], 50)

        self.assertEqual(events[2][0], EventTag.ParticipantSpawned)
        self.assertEqual(events[2][1]["player"], 1)
        self.assertEqual(events[2][1]["x"], 50)
        self.assertEqual(events[2][1]["y"], 50)
        self.assertEqual(events[2][1]["hp"], 100)

        self.assertEqual(
            events[3],
            (
                EventTag.ProjectileSpawned,
                {"projectile": 7, "owner": 1, "skill": 2, "target": 10, "x": 50, "y": 50},
            ),
        )
        self.assertEqual(events[4], (EventTag.ProjectileMoved, {"projectile": 7, "x": 50, "y": 46}))
        self.assertEqual(events[5][0], EventTag.ProjectileRemoved)
        self.assertEqual(events[5][1]["reason"], int(ProjectileRemovalReason.Hit))

    def test_push_drain_does_not_steal_a_pending_request_response(self) -> None:
        session = Session()
        response = snf_wire.Frame(MessageType.RoomJoined, 7, b"response")
        session._queue.put(response)

        session._receive_lock.acquire()
        try:
            self.assertEqual(session.drain_pushes(), [])
        finally:
            session._receive_lock.release()

        self.assertEqual(session.drain_pushes(), [response])


class TestWorldState(unittest.TestCase):
    def test_zone_state_transitions(self) -> None:
        world = World()
        self.assertEqual(world.mode, "zone")
        world.apply_zone_state(zone_id=1, epoch=1, x=50, y=-50, visible_players=[2, 3])
        self.assertEqual(world.zone_x, 50)
        self.assertEqual(world.zone_y, -50)
        self.assertEqual(world.zone_visible_players, [2, 3])
        self.assertIn(2, world.zone_peers)
        self.assertIn(3, world.zone_peers)

        p2_pos = world.get_zone_peer_pos(2, now=10.0)
        self.assertIsInstance(p2_pos, tuple)
        self.assertEqual(len(p2_pos), 2)

        world.apply_zone_state(zone_id=1, epoch=2, x=50, y=-50, visible_players=[2])
        self.assertIn(2, world.zone_peers)
        self.assertNotIn(3, world.zone_peers)

        world.enter_room_mode()
        self.assertEqual(world.mode, "room")

        world.return_to_zone_mode(zone_id=1, x=50, y=-50)
        self.assertEqual(world.mode, "zone")
        self.assertEqual(world.zone_x, 50)
        self.assertEqual(world.zone_y, -50)

    def test_applied_zone_move_does_not_rewind_local_prediction(self) -> None:
        response = {"status": int(ZoneCommandStatus.Applied), "x": 8, "y": -4}

        position = zone_render_position_after_response(10.75, -2.25, MessageType.Moved, response)

        self.assertEqual(position, (10.75, -2.25))

    def test_zone_entry_and_rejected_move_snap_to_server_position(self) -> None:
        entered = {"status": int(ZoneCommandStatus.Applied), "x": 8, "y": -4}
        rejected = {"status": int(ZoneCommandStatus.StaleRoute), "x": 6, "y": -2}

        self.assertEqual(
            zone_render_position_after_response(10.75, -2.25, MessageType.ZoneEntered, entered),
            (8.0, -4.0),
        )
        self.assertEqual(
            zone_render_position_after_response(10.75, -2.25, MessageType.Moved, rejected),
            (6.0, -2.0),
        )

    def test_world_digest_application(self) -> None:
        world = World()
        events = [
            (EventTag.ArenaStarted, {"width": 120, "height": 120}),
            (EventTag.EnemySpawned, {"id": 1, "kind": int(EnemyKind.Boss), "hp": 1000}),
            (EventTag.ParticipantSpawned, {"player": 1, "x": 60, "y": 60, "hp": 100}),
            (EventTag.ProjectileSpawned, {"projectile": 7, "owner": 1, "skill": 2, "target": 1, "x": 60, "y": 60}),
        ]
        world.apply_digest(1, RoomPhase.Running, events, now=10.0)

        self.assertEqual(world.arena_w, 120)
        self.assertEqual(world.arena_h, 120)
        self.assertEqual(world.boss_id, 1)
        self.assertEqual(len(world.enemies), 1)
        self.assertEqual(world.enemies[1].hp, 1000)
        self.assertEqual(world.enemies[1].kind, EnemyKind.Boss)
        self.assertEqual(len(world.players), 1)
        self.assertEqual(world.players[1].x, 60.0)
        self.assertEqual(world.projectiles[7].target, 1)

        events2 = [
            (EventTag.ParticipantMoved, {"player": 1, "x": 64, "y": 60}),
            (EventTag.ProjectileMoved, {"projectile": 7, "x": 60, "y": 56}),
            (EventTag.EnemyDamaged, {"target": 1, "actor": 1, "skill": 1, "amount": 10, "hp": 990}),
        ]
        world.apply_digest(2, RoomPhase.Running, events2, now=10.05)
        self.assertEqual(world.players[1].x, 64.0)
        self.assertEqual(world.enemies[1].hp, 990)
        self.assertEqual(world.projectiles[7].y, 56.0)
        projectile_x, projectile_y = world.projectiles[7].interpolated_pos(now=10.1)
        self.assertAlmostEqual(projectile_x, 60.0)
        self.assertAlmostEqual(projectile_y, 58.0)

        world.apply_digest(
            3,
            RoomPhase.Running,
            [
                (EventTag.ProjectileSpawned, {"projectile": 8, "owner": 1, "skill": 2, "target": 1, "x": 60, "y": 60}),
                (EventTag.ProjectileSpawned, {"projectile": 9, "owner": 1, "skill": 2, "target": 1, "x": 60, "y": 60}),
            ],
        )
        world.apply_digest(
            4,
            RoomPhase.Running,
            [
                (EventTag.ProjectileRemoved, {"projectile": 7, "reason": int(ProjectileRemovalReason.Hit)}),
                (EventTag.ProjectileRemoved, {"projectile": 8, "reason": int(ProjectileRemovalReason.TargetLost)}),
                (EventTag.ProjectileRemoved, {"projectile": 9, "reason": int(ProjectileRemovalReason.Expired)}),
            ],
        )
        self.assertEqual(world.projectiles, {})

        world.apply_digest(
            5,
            RoomPhase.Running,
            [(EventTag.ProjectileSpawned, {"projectile": 10, "owner": 1, "skill": 2, "target": 1, "x": 60, "y": 60})],
        )
        world.apply_digest(6, RoomPhase.Failed, [])
        self.assertEqual(world.projectiles, {})


class TestBotDefaults(unittest.TestCase):
    def test_bot_uses_arcane_bolt_by_default(self) -> None:
        bot = BotPlayer(player_id=2, room_id=1)
        self.assertEqual(bot.attack_skill_id, snf_wire.ARCANE_BOLT_SKILL_ID)
        self.assertEqual(bot.attack_interval, 1.5)

    def test_party_reentry_joins_bots_before_main_starts_battle(self) -> None:
        calls: list[tuple[str, bool]] = []

        class FakeBot:
            def join_room(self, room_id: int, start: bool) -> bool:
                self.room_id = room_id
                calls.append(("bot", start))
                return True

        class FakeSession:
            in_room = False

            def join_room(self, room_id: int, start: bool) -> None:
                self.room_id = room_id
                calls.append(("main", start))
                self.in_room = True

        world = World()
        joined = join_party_room(FakeSession(), world, [FakeBot(), FakeBot(), FakeBot()], room_id=1, start=True)

        self.assertTrue(joined)
        self.assertEqual(calls, [("bot", False), ("bot", False), ("bot", False), ("main", True)])
        self.assertEqual(world.mode, "room")

    def test_party_reentry_rejection_does_not_change_world_mode(self) -> None:
        class RejectingSession:
            in_room = False

            def join_room(self, room_id: int, start: bool) -> None:
                raise RuntimeError("WrongPhase")

        world = World()
        joined = join_party_room(RejectingSession(), world, [], room_id=1, start=True)

        self.assertFalse(joined)
        self.assertEqual(world.mode, "zone")
        self.assertIn("WrongPhase", world.log[-1])


if __name__ == "__main__":
    unittest.main()
