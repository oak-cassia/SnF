from __future__ import annotations

import struct
import unittest

import snf_wire
from snf_wire import (
    BattleFailureReason,
    Direction,
    EnemyKind,
    EventTag,
    FrameDecoder,
    MessageType,
    RoomPhase,
    RoomStatus,
)
from snf_world import World


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
        # Feed partial 1: only length header
        decoder.feed(encoded[:4])
        self.assertEqual(list(decoder.frames()), [])

        # Feed partial 2: some of the body
        decoder.feed(encoded[4:8])
        self.assertEqual(list(decoder.frames()), [])

        # Feed partial 3: the rest
        decoder.feed(encoded[8:])
        frames = list(decoder.frames())
        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0].type, MessageType.Ping)
        self.assertEqual(frames[0].request_id, 101)
        self.assertEqual(frames[0].payload, payload)

    def test_request_builders(self) -> None:
        # Authenticate
        auth = snf_wire.authenticate(12345)
        self.assertEqual(len(auth), 8)
        self.assertEqual(struct.unpack(">Q", auth)[0], 12345)

        # EnterZone
        zone = snf_wire.enter_zone(1, -100, 200)
        self.assertEqual(len(zone), 16)
        zid, zx, zy = struct.unpack(">Qii", zone)
        self.assertEqual(zid, 1)
        self.assertEqual(zx, -100)
        self.assertEqual(zy, 200)

        # RoomJoin & BattleStart
        rjoin = snf_wire.room_join(7)
        self.assertEqual(len(rjoin), 8)
        bstart = snf_wire.battle_start(7)
        self.assertEqual(len(bstart), 8)

        # SetMoveIntent
        move = snf_wire.set_move_intent(7, Direction.NorthEast, 5)
        self.assertEqual(len(move), 17)
        rm, d, sq = struct.unpack(">QBQ", move)
        self.assertEqual(rm, 7)
        self.assertEqual(d, int(Direction.NorthEast))
        self.assertEqual(sq, 5)

        # UseSkill
        skill = snf_wire.use_skill(7, 1, 10)
        self.assertEqual(len(skill), 20)
        rm, sk, sq = struct.unpack(">QIQ", skill)
        self.assertEqual(rm, 7)
        self.assertEqual(sk, 1)
        self.assertEqual(sq, 10)

    def test_response_parsers(self) -> None:
        # parse_authenticated
        self.assertEqual(snf_wire.parse_authenticated(struct.pack(">Q", 99)), 99)

        # parse_room_reply
        status, phase, room = snf_wire.parse_room_reply(
            struct.pack(">BBQ", int(RoomStatus.Applied), int(RoomPhase.Running), 77)
        )
        self.assertEqual(status, RoomStatus.Applied)
        self.assertEqual(phase, RoomPhase.Running)
        self.assertEqual(room, 77)

        # parse_ack
        status, phase = snf_wire.parse_ack(
            struct.pack(">BB", int(RoomStatus.Applied), int(RoomPhase.Running))
        )
        self.assertEqual(status, RoomStatus.Applied)
        self.assertEqual(phase, RoomPhase.Running)

        # parse_cleared
        self.assertEqual(snf_wire.parse_cleared(struct.pack(">Q", 500)), 500)

        # parse_failed
        hp, spawned, reason = snf_wire.parse_failed(
            struct.pack(">QBB", 250, 1, int(BattleFailureReason.ParticipantsDefeated))
        )
        self.assertEqual(hp, 250)
        self.assertTrue(spawned)
        self.assertEqual(reason, BattleFailureReason.ParticipantsDefeated)

        # parse_returned
        rz, rx, ry = snf_wire.parse_returned(struct.pack(">Qii", 1, 10, -20))
        self.assertEqual(rz, 1)
        self.assertEqual(rx, 10)
        self.assertEqual(ry, -20)

    def test_parse_digest(self) -> None:
        # Build raw digest payload: seq=1, phase=Running(1), count=3
        # Event 1: ArenaStarted(4) -> width=100, height=100
        # Event 2: EnemySpawned(0) -> id=10, kind=Minion(0), hp=50
        # Event 3: ParticipantSpawned(5) -> player=1, x=50, y=50, hp=100
        buf = bytearray()
        buf.extend(struct.pack(">QBH", 1, int(RoomPhase.Running), 3))

        # Ev 1
        buf.append(int(EventTag.ArenaStarted))
        buf.extend(struct.pack(">II", 100, 100))

        # Ev 2
        buf.append(int(EventTag.EnemySpawned))
        buf.extend(struct.pack(">IBQ", 10, int(EnemyKind.Minion), 50))

        # Ev 3
        buf.append(int(EventTag.ParticipantSpawned))
        buf.extend(struct.pack(">QIIQ", 1, 50, 50, 100))

        seq, phase, events = snf_wire.parse_digest(bytes(buf))
        self.assertEqual(seq, 1)
        self.assertEqual(phase, RoomPhase.Running)
        self.assertEqual(len(events), 3)

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

        # Peer positions are calculated and have smooth motion
        p2_pos = world.get_zone_peer_pos(2, now=10.0)
        self.assertIsInstance(p2_pos, tuple)
        self.assertEqual(len(p2_pos), 2)

        # Disappearance of peer
        world.apply_zone_state(zone_id=1, epoch=2, x=50, y=-50, visible_players=[2])
        self.assertIn(2, world.zone_peers)
        self.assertNotIn(3, world.zone_peers)

        # Enter room
        world.enter_room_mode()
        self.assertEqual(world.mode, "room")

        # Return to zone
        world.return_to_zone_mode(zone_id=1, x=50, y=-50)
        self.assertEqual(world.mode, "zone")
        self.assertEqual(world.zone_x, 50)
        self.assertEqual(world.zone_y, -50)

    def test_world_digest_application(self) -> None:
        world = World()
        events = [
            (EventTag.ArenaStarted, {"width": 120, "height": 120}),
            (EventTag.EnemySpawned, {"id": 1, "kind": int(EnemyKind.Boss), "hp": 1000}),
            (EventTag.ParticipantSpawned, {"player": 1, "x": 60, "y": 60, "hp": 100}),
        ]
        world.apply_digest(1, RoomPhase.Running, events)

        self.assertEqual(world.arena_w, 120)
        self.assertEqual(world.arena_h, 120)
        self.assertEqual(world.boss_id, 1)
        self.assertEqual(len(world.enemies), 1)
        self.assertEqual(world.enemies[1].hp, 1000)
        self.assertEqual(world.enemies[1].kind, EnemyKind.Boss)
        self.assertEqual(len(world.players), 1)
        self.assertEqual(world.players[1].x, 60.0)

        # Apply movement & damage
        events2 = [
            (EventTag.ParticipantMoved, {"player": 1, "x": 64, "y": 60}),
            (EventTag.EnemyDamaged, {"target": 1, "actor": 1, "skill": 1, "amount": 10, "hp": 990}),
        ]
        world.apply_digest(2, RoomPhase.Running, events2)
        self.assertEqual(world.players[1].x, 64.0)
        self.assertEqual(world.enemies[1].hp, 990)


if __name__ == "__main__":
    unittest.main()
