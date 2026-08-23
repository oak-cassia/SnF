from __future__ import annotations

import argparse
import random
import threading
import time
from typing import Optional

import snf_wire
from snf_session import Session
from snf_wire import Direction, EnemyKind, MessageType, RoomPhase
from snf_world import World


class BotPlayer:
    def __init__(
        self,
        player_id: int,
        room_id: int,
        zone_id: int = 1,
        host: str = "127.0.0.1",
        port: int = 7777,
        attack_interval: float = 1.0,
        zone_first: bool = False,
    ) -> None:
        self.player_id = player_id
        self.room_id = room_id
        self.zone_id = zone_id
        self.host = host
        self.port = port
        self.attack_interval = attack_interval
        self.zone_first = zone_first

        self.session = Session(host=host, port=port)
        self.world = World()
        self.running = False
        self.thread: Optional[threading.Thread] = None

        self.last_attack_time = 0.0
        self.last_move_time = 0.0
        self.move_interval = 1.0
        self.current_direction = Direction.Stop

        # Zone wandering
        self.bot_x = 0.0
        self.bot_y = 0.0
        self.last_zone_move_time = 0.0
        self.zone_move_interval = 1.5

    def start(self) -> None:
        self.running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.running = False
        if self.session.in_room:
            try:
                self.session.send_room_leave()
            except Exception:
                pass
        self.session.close()
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=1.0)

    def join_room(self, room_id: int | None = None, start: bool = False) -> None:
        target_room = room_id if room_id is not None else self.room_id
        if self.session.in_zone and not self.session.in_room:
            try:
                self.session.join_room(room_id=target_room, start=start)
                self.world.enter_room_mode()
                print(f"[Bot P{self.player_id}] Joined Room #{target_room} from Zone.")
            except Exception as e:
                print(f"[Bot P{self.player_id}] Join room failed: {e}")

    def leave_room(self) -> None:
        if self.session.in_room:
            try:
                self.session.send_room_leave()
                print(f"[Bot P{self.player_id}] Left Room #{self.room_id} back to Zone.")
            except Exception as e:
                print(f"[Bot P{self.player_id}] Leave room failed: {e}")

    def _choose_direction(self) -> Direction:
        my_p = self.world.players.get(self.player_id)
        if not my_p or not my_p.alive:
            return Direction.Stop

        # 70% chance to move towards the closest enemy if available
        if self.world.enemies and random.random() < 0.7:
            # Find closest enemy (boss prioritized if exists)
            target = None
            if self.world.boss_id and self.world.boss_id in self.world.enemies:
                target = self.world.enemies[self.world.boss_id]
            else:
                closest_dist = float("inf")
                for enemy in self.world.enemies.values():
                    dist = (enemy.x - my_p.x) ** 2 + (enemy.y - my_p.y) ** 2
                    if dist < closest_dist:
                        closest_dist = dist
                        target = enemy

            if target:
                dx = 0
                dy = 0
                if target.x > my_p.x + 2:
                    dx = 1
                elif target.x < my_p.x - 2:
                    dx = -1

                if target.y > my_p.y + 2:
                    dy = 1
                elif target.y < my_p.y - 2:
                    dy = -1

                # Map (dx, dy) to Direction
                dir_map = {
                    (0, 0): Direction.Stop,
                    (0, -1): Direction.North,
                    (1, -1): Direction.NorthEast,
                    (1, 0): Direction.East,
                    (1, 1): Direction.SouthEast,
                    (0, 1): Direction.South,
                    (-1, 1): Direction.SouthWest,
                    (-1, 0): Direction.West,
                    (-1, -1): Direction.NorthWest,
                }
                return dir_map.get((dx, dy), Direction.Stop)

        # Otherwise pick a random direction
        return Direction(random.randint(0, 8))

    def _run_loop(self) -> None:
        import math

        try:
            self.session.connect()
            if self.zone_first:
                # Compute initial position around portal
                angle = (self.player_id * 137.5) * math.pi / 180.0
                dist = 28.0 + (self.player_id % 4) * 16.0
                spawn_x = int(math.cos(angle) * dist)
                spawn_y = int(math.sin(angle) * dist)
                self.bot_x = float(spawn_x)
                self.bot_y = float(spawn_y)
                z_data = self.session.bootstrap_zone(
                    player_id=self.player_id,
                    zone_id=self.zone_id,
                    x=spawn_x,
                    y=spawn_y,
                )
                self.world.apply_zone_state(
                    zone_id=z_data["zone_id"],
                    epoch=z_data["epoch"],
                    x=z_data["x"],
                    y=z_data["y"],
                    visible_players=z_data["visible_players"],
                )
                print(f"[Bot P{self.player_id}] Entered Zone #{self.zone_id} at ({spawn_x}, {spawn_y}).")
            else:
                self.session.bootstrap(
                    player_id=self.player_id,
                    zone_id=self.zone_id,
                    room_id=self.room_id,
                    start=False,
                )
                print(f"[Bot P{self.player_id}] Joined Room #{self.room_id}. Ready for battle!")

            while self.running and self.session.is_connected:
                now = time.monotonic()

                # 1. Drain pushes & update world state
                pushes = self.session.drain_pushes()
                for frame in pushes:
                    if frame.type in (MessageType.Moved, MessageType.ZoneEntered):
                        z_data = snf_wire.parse_moved(frame.payload)
                        self.world.apply_zone_state(
                            zone_id=z_data["zone_id"],
                            epoch=z_data["epoch"],
                            x=z_data["x"],
                            y=z_data["y"],
                            visible_players=z_data["visible_players"],
                            now=now,
                        )
                        self.bot_x = float(z_data["x"])
                        self.bot_y = float(z_data["y"])
                    elif frame.type == MessageType.BattleDigest:
                        seq, phase, events = snf_wire.parse_digest(frame.payload)
                        self.world.apply_digest(seq, phase, events, now=now)
                    elif frame.type == MessageType.BattleCleared:
                        self.world.phase = RoomPhase.Cleared
                    elif frame.type == MessageType.BattleFailed:
                        self.world.phase = RoomPhase.Failed
                    elif frame.type == MessageType.ReturnedToZone:
                        rz, rx, ry = snf_wire.parse_returned(frame.payload)
                        self.world.return_to_zone_mode(rz, rx, ry)
                        self.bot_x = float(rx)
                        self.bot_y = float(ry)

                # 2. Zone wandering AI
                if self.session.in_zone and not self.session.in_room:
                    if now - self.last_zone_move_time >= self.zone_move_interval:
                        wander_dx = random.choice([-8, -4, 0, 4, 8])
                        wander_dy = random.choice([-8, -4, 0, 4, 8])
                        self.bot_x += wander_dx
                        self.bot_y += wander_dy
                        # Keep within 80 units of spawn/portal
                        if math.hypot(self.bot_x, self.bot_y) > 80:
                            self.bot_x *= 0.85
                            self.bot_y *= 0.85
                        self.session.send_zone_move(int(self.bot_x), int(self.bot_y))
                        self.last_zone_move_time = now
                        self.zone_move_interval = random.uniform(1.2, 2.5)

                # 3. Battle action loop
                elif self.world.phase == RoomPhase.Running and self.session.in_room:
                    my_p = self.world.players.get(self.player_id)
                    if my_p and my_p.alive:
                        # Move AI: change direction every move_interval
                        if now - self.last_move_time >= self.move_interval:
                            new_dir = self._choose_direction()
                            if new_dir != self.current_direction:
                                self.current_direction = new_dir
                                self.session.send_move_intent(new_dir)
                            self.last_move_time = now
                            self.move_interval = random.uniform(0.4, 1.2)

                        # Attack AI: cast SLASH every attack_interval (1.0s)
                        if now - self.last_attack_time >= self.attack_interval:
                            self.session.send_use_skill(1)
                            self.last_attack_time = now

                time.sleep(0.05)

        except Exception as e:
            if self.running:
                print(f"[Bot P{self.player_id}] Stopped: {e}")
        finally:
            self.session.close()


def spawn_bots(
    count: int = 3,
    start_id: int = 2,
    room_id: int = 1,
    zone_id: int = 1,
    host: str = "127.0.0.1",
    port: int = 7777,
    zone_first: bool = False,
) -> list[BotPlayer]:
    bots = []
    for idx in range(count):
        pid = start_id + idx
        bot = BotPlayer(
            player_id=pid,
            room_id=room_id,
            zone_id=zone_id,
            host=host,
            port=port,
            zone_first=zone_first,
        )
        bot.start()
        bots.append(bot)
        time.sleep(0.05)  # Stagger connect slightly
    return bots


def main() -> None:
    parser = argparse.ArgumentParser(description="SnF AI Bot Players Runner")
    parser.add_argument("--host", default="127.0.0.1", help="Server host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=7777, help="Server port (default: 7777)")
    parser.add_argument("--room", type=int, default=1, help="Room ID (default: 1)")
    parser.add_argument("--zone", type=int, default=1, help="Zone ID (default: 1)")
    parser.add_argument("--count", type=int, default=3, help="Number of bots to spawn (default: 3)")
    parser.add_argument("--start-id", type=int, default=2, help="Starting Player ID for bots (default: 2)")
    parser.add_argument("--zone-first", action="store_true", help="Start bots in Zone open field first")

    args = parser.parse_args()

    mode_str = "Zone #{}".format(args.zone) if args.zone_first else "Room #{}".format(args.room)
    print(f"Spawning {args.count} bot(s) starting from Player #{args.start_id} into {mode_str}...")
    bots = spawn_bots(
        count=args.count,
        start_id=args.start_id,
        room_id=args.room,
        zone_id=args.zone,
        host=args.host,
        port=args.port,
        zone_first=args.zone_first,
    )

    print(f"All {len(bots)} bot(s) running. Press Ctrl+C to terminate bots.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping bots...")
    finally:
        for bot in bots:
            bot.stop()
        print("All bots terminated.")


if __name__ == "__main__":
    main()
