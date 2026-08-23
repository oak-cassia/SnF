from __future__ import annotations

import argparse
import math
import sys
import time

import snf_wire
from snf_session import Session
from snf_wire import Direction, EnemyKind, MessageType, RoomPhase
from snf_world import World


def run_headless(
    host: str,
    port: int,
    player_id: int,
    zone_id: int,
    room_id: int,
    start: bool,
    boss_seconds: float,
    battle_seconds: float,
    bots: int = 0,
    zone_first: bool = False,
) -> None:
    bot_list = []

    session = Session(host=host, port=port)
    world = World()

    print("=" * 60)
    print("SnF Headless Client")
    print(f"Connecting to {host}:{port} ...")
    session.connect()
    print("Connected.")

    if zone_first:
        print(f"Bootstrapping into Zone #{zone_id} (Player #{player_id}) ...")
        if bots > 0:
            from snf_bot import spawn_bots
            bot_list = spawn_bots(
                count=bots,
                start_id=player_id + 1,
                room_id=room_id,
                zone_id=zone_id,
                host=host,
                port=port,
                zone_first=True,
            )

        zone_data = session.bootstrap_zone(player_id=player_id, zone_id=zone_id, x=0, y=0)
        world.apply_zone_state(
            zone_id=zone_data["zone_id"],
            epoch=zone_data["epoch"],
            x=zone_data["x"],
            y=zone_data["y"],
            visible_players=zone_data["visible_players"],
        )
        print(f"Zone entered. Position: ({world.zone_x}, {world.zone_y}), Visible peers: {len(world.zone_visible_players)}")
    else:
        print(f"Bootstrapping: Player={player_id}, Zone={zone_id}, Room={room_id}, Start={start}")
        if bots > 0:
            from snf_bot import spawn_bots
            bot_list = spawn_bots(
                count=bots,
                start_id=player_id + 1,
                room_id=room_id,
                zone_id=zone_id,
                host=host,
                port=port,
                zone_first=False,
            )

        session.bootstrap(
            player_id=player_id,
            zone_id=zone_id,
            room_id=room_id,
            start=start,
            auto_join_room=True,
        )
        world.enter_room_mode()
        print(f"Bootstrap complete. in_room={session.in_room}")

    print("Listening for server events... (Ctrl+C to quit)")
    print("=" * 60)

    last_summary_time = 0.0
    last_log_count = 0
    zone_step_count = 0

    try:
        while session.is_connected:
            now = time.monotonic()
            pushes = session.drain_pushes()
            for frame in pushes:
                if frame.type == MessageType.Moved or frame.type == MessageType.ZoneEntered:
                    z_data = snf_wire.parse_moved(frame.payload)
                    world.apply_zone_state(
                        zone_id=z_data["zone_id"],
                        epoch=z_data["epoch"],
                        x=z_data["x"],
                        y=z_data["y"],
                        visible_players=z_data["visible_players"],
                        now=now,
                    )
                elif frame.type == MessageType.BattleDigest:
                    seq, phase, events = snf_wire.parse_digest(frame.payload)
                    world.apply_digest(seq, phase, events, now=now)
                elif frame.type == MessageType.BattleCleared:
                    exp = snf_wire.parse_cleared(frame.payload)
                    world.phase = RoomPhase.Cleared
                    world.cleared_exp = exp
                    world.add_log(f"BATTLE CLEARED! Experience +{exp}")
                elif frame.type == MessageType.BattleFailed:
                    boss_hp, boss_spawned, reason = snf_wire.parse_failed(frame.payload)
                    world.phase = RoomPhase.Failed
                    world.failure_reason = reason.name
                    world.boss_final_hp = boss_hp
                    world.add_log(f"BATTLE FAILED! Reason={reason.name}, Boss HP={boss_hp}")
                elif frame.type == MessageType.ReturnedToZone:
                    ret_zone, rx, ry = snf_wire.parse_returned(frame.payload)
                    world.return_to_zone_mode(ret_zone, rx, ry)
                    world.add_log(f"Returned to Zone {ret_zone} at ({rx}, {ry})")

            # In Zone mode: take periodic movement steps in headless mode
            if world.mode == "zone" and session.in_zone:
                if now - last_summary_time >= 1.0:
                    zone_step_count += 1
                    target_x = zone_step_count * 10
                    target_y = zone_step_count * 5
                    session.send_zone_move(target_x, target_y)

            # Print newly added logs
            if len(world.log) > last_log_count:
                for idx in range(last_log_count, len(world.log)):
                    print(f"  [LOG] {world.log[idx]}")
                last_log_count = len(world.log)

            # Periodic status print (1Hz)
            if now - last_summary_time >= 1.0:
                last_summary_time = now
                if world.mode == "zone":
                    peer_str = f" ({[f'P{p}' for p in world.zone_visible_players]})" if world.zone_visible_players else ""
                    print(
                        f"[{time.strftime('%H:%M:%S')}] P{player_id} | Mode: ZONE #{world.zone_id} | "
                        f"Pos: ({world.zone_x}, {world.zone_y}) | Visible Peers: {len(world.zone_visible_players)}{peer_str}"
                    )
                else:
                    boss_str = "None"
                    if world.boss_id and world.boss_id in world.enemies:
                        boss = world.enemies[world.boss_id]
                        boss_str = f"HP {boss.hp}/{boss.max_hp}"
                    elif world.boss_final_hp is not None:
                        boss_str = f"Final HP {world.boss_final_hp}"

                    elapsed_str = ""
                    if world.battle_start_time is not None:
                        elapsed = now - world.battle_start_time
                        elapsed_str = f" Elapsed={elapsed:.1f}s (Boss in {max(0.0, boss_seconds - elapsed):.1f}s, Deadline in {max(0.0, battle_seconds - elapsed):.1f}s [assumed])"

                    print(
                        f"[{time.strftime('%H:%M:%S')}] P{player_id} R{room_id} | "
                        f"Phase: {world.phase.name} | DigestSeq: {world.digest_seq} | "
                        f"Players: {len(world.players)} | Enemies: {len(world.enemies)} | "
                        f"Boss: {boss_str}{elapsed_str}"
                    )

            if world.phase in (RoomPhase.Cleared, RoomPhase.Failed) and world.returned_zone is not None:
                print(f"Battle ended ({world.phase.name}) and returned to zone.")
                if not zone_first:
                    break

            time.sleep(0.05)

    except KeyboardInterrupt:
        print("\nInterrupted by user.")
    finally:
        if session.in_room:
            session.send_room_leave()
        session.close()
        for b in bot_list:
            b.stop()
        print("Session closed.")


def run_gui(
    host: str,
    port: int,
    player_id: int,
    zone_id: int,
    room_id: int,
    start: bool,
    scale: int,
    boss_seconds: float,
    battle_seconds: float,
    bots: int = 0,
    zone_first: bool = False,
) -> None:
    try:
        import pygame
    except ImportError:
        print("pygame is not installed. Please install it with 'pip install pygame' or run with '--headless'.", file=sys.stderr)
        sys.exit(1)

    bot_list = []
    session = Session(host=host, port=port)
    world = World()

    session.connect()

    if zone_first:
        if bots > 0:
            from snf_bot import spawn_bots
            bot_list = spawn_bots(
                count=bots,
                start_id=player_id + 1,
                room_id=room_id,
                zone_id=zone_id,
                host=host,
                port=port,
                zone_first=True,
            )

        zone_data = session.bootstrap_zone(
            player_id=player_id,
            zone_id=zone_id,
            x=0,
            y=0,
        )
        world.apply_zone_state(
            zone_id=zone_data["zone_id"],
            epoch=zone_data["epoch"],
            x=zone_data["x"],
            y=zone_data["y"],
            visible_players=zone_data["visible_players"],
        )
    else:
        if bots > 0:
            from snf_bot import spawn_bots
            bot_list = spawn_bots(
                count=bots,
                start_id=player_id + 1,
                room_id=room_id,
                zone_id=zone_id,
                host=host,
                port=port,
                zone_first=False,
            )

        session.bootstrap(
            player_id=player_id,
            zone_id=zone_id,
            room_id=room_id,
            start=start,
            auto_join_room=True,
        )
        world.enter_room_mode()

    pygame.init()
    pygame.font.init()

    arena_px_w = world.arena_w * scale
    arena_px_h = world.arena_h * scale
    panel_w = 240
    win_w = arena_px_w + panel_w
    win_h = max(arena_px_h, 600)

    screen = pygame.display.set_mode((win_w, win_h))
    pygame.display.set_caption(f"SnF Client - Player {player_id}")
    clock = pygame.time.Clock()

    font_large = pygame.font.SysFont("Helvetica,Arial,sans-serif", 20, bold=True)
    font_medium = pygame.font.SysFont("Helvetica,Arial,sans-serif", 15, bold=True)
    font_small = pygame.font.SysFont("Helvetica,Arial,sans-serif", 12)
    font_banner = pygame.font.SysFont("Helvetica,Arial,sans-serif", 32, bold=True)

    current_direction = Direction.Stop
    last_skill_time = 0.0
    slash_cooldown = 1.0
    slash_range = 12

    # Zone movement tracking
    zone_local_x = float(world.zone_x)
    zone_local_y = float(world.zone_y)
    last_zone_move_time = 0.0

    running = True

    while running and session.is_connected:
        now = time.monotonic()

        # Handle events
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                running = False
            elif event.type == pygame.KEYDOWN:
                if event.key == pygame.K_ESCAPE:
                    if world.mode == "room" and session.in_room:
                        # Leave room back to zone
                        session.send_room_leave()
                        for b in bot_list:
                            b.leave_room()
                    else:
                        running = False

                # Enter Room from Zone
                elif event.key in (pygame.K_j, pygame.K_RETURN):
                    if world.mode == "zone" and session.in_zone and not session.in_room:
                        if bots > 0:
                            if len(bot_list) == 0:
                                from snf_bot import spawn_bots
                                bot_list = spawn_bots(
                                    count=bots,
                                    start_id=player_id + 1,
                                    room_id=room_id,
                                    zone_id=zone_id,
                                    host=host,
                                    port=port,
                                    zone_first=False,
                                )
                            else:
                                for b in bot_list:
                                    b.join_room(room_id=room_id, start=start)
                        session.join_room(room_id=room_id, start=start)
                        world.enter_room_mode()

                # Start Battle in Room
                elif event.key == pygame.K_r:
                    if world.mode == "room" and world.phase == RoomPhase.Waiting and session.in_room:
                        session.send_battle_start()

                # Slash Attack in Room
                elif event.key == pygame.K_SPACE:
                    if world.mode == "room" and session.in_room and now - last_skill_time >= slash_cooldown:
                        my_entity = world.players.get(player_id)
                        if my_entity and my_entity.alive:
                            session.send_use_skill(1)
                            last_skill_time = now

        # Keys pressed
        keys = pygame.key.get_pressed()
        dx = 0
        dy = 0
        if keys[pygame.K_w] or keys[pygame.K_UP]:
            dy -= 1
        if keys[pygame.K_s] or keys[pygame.K_DOWN]:
            dy += 1
        if keys[pygame.K_a] or keys[pygame.K_LEFT]:
            dx -= 1
        if keys[pygame.K_d] or keys[pygame.K_RIGHT]:
            dx += 1

        # Movement in Zone Mode
        if world.mode == "zone" and session.in_zone:
            if dx != 0 or dy != 0:
                zone_speed = 35.0  # units per second
                dt = clock.get_time() / 1000.0
                zone_local_x += dx * zone_speed * dt
                zone_local_y += dy * zone_speed * dt

                if now - last_zone_move_time >= 0.1:  # 10Hz throttle
                    session.send_zone_move(int(zone_local_x), int(zone_local_y))
                    last_zone_move_time = now
                    world.zone_x = int(zone_local_x)
                    world.zone_y = int(zone_local_y)
            elif now - last_zone_move_time >= 1.0:  # 1Hz idle heartbeat to refresh visible peers
                session.send_zone_move(int(zone_local_x), int(zone_local_y))
                last_zone_move_time = now

        # Movement in Room Arena Mode (Continuous Move Intent)
        elif world.mode == "room":
            new_dir = Direction.Stop
            if dx == 0 and dy == -1:
                new_dir = Direction.North
            elif dx == 1 and dy == -1:
                new_dir = Direction.NorthEast
            elif dx == 1 and dy == 0:
                new_dir = Direction.East
            elif dx == 1 and dy == 1:
                new_dir = Direction.SouthEast
            elif dx == 0 and dy == 1:
                new_dir = Direction.South
            elif dx == -1 and dy == 1:
                new_dir = Direction.SouthWest
            elif dx == -1 and dy == 0:
                new_dir = Direction.West
            elif dx == -1 and dy == -1:
                new_dir = Direction.NorthWest

            if new_dir != current_direction:
                current_direction = new_dir
                if session.in_room:
                    session.send_move_intent(new_dir)

        # Drain pushes & update world
        pushes = session.drain_pushes()
        for frame in pushes:
            if frame.type in (MessageType.Moved, MessageType.ZoneEntered):
                z_data = snf_wire.parse_moved(frame.payload)
                world.apply_zone_state(
                    zone_id=z_data["zone_id"],
                    epoch=z_data["epoch"],
                    x=z_data["x"],
                    y=z_data["y"],
                    visible_players=z_data["visible_players"],
                    now=now,
                )
                zone_local_x = float(z_data["x"])
                zone_local_y = float(z_data["y"])
            elif frame.type == MessageType.BattleDigest:
                seq, phase, events = snf_wire.parse_digest(frame.payload)
                world.apply_digest(seq, phase, events, now=now)
            elif frame.type == MessageType.BattleCleared:
                exp = snf_wire.parse_cleared(frame.payload)
                world.phase = RoomPhase.Cleared
                world.cleared_exp = exp
                world.add_log(f"CLEARED! Exp +{exp}")
            elif frame.type == MessageType.BattleFailed:
                boss_hp, boss_spawned, reason = snf_wire.parse_failed(frame.payload)
                world.phase = RoomPhase.Failed
                world.failure_reason = reason.name
                world.boss_final_hp = boss_hp
                world.add_log(f"FAILED! {reason.name}")
            elif frame.type == MessageType.ReturnedToZone:
                ret_zone, rx, ry = snf_wire.parse_returned(frame.payload)
                world.return_to_zone_mode(ret_zone, rx, ry)
                zone_local_x = float(rx)
                zone_local_y = float(ry)

        # Dynamic window resize if arena size changed from default
        cur_arena_w = world.arena_w * scale
        cur_arena_h = world.arena_h * scale
        if cur_arena_w != arena_px_w or cur_arena_h != arena_px_h:
            arena_px_w = cur_arena_w
            arena_px_h = cur_arena_h
            win_w = arena_px_w + panel_w
            win_h = max(arena_px_h, 600)
            screen = pygame.display.set_mode((win_w, win_h))

        # --- RENDERING ---
        screen.fill((18, 20, 28))

        # ==========================
        # VIEW 1: ZONE FIELD MODE
        # ==========================
        if world.mode == "zone":
            field_rect = pygame.Rect(0, 0, arena_px_w, arena_px_h)
            pygame.draw.rect(screen, (20, 32, 28), field_rect)

            # Zone field camera centered on player
            cam_center_x = arena_px_w // 2
            cam_center_y = arena_px_h // 2

            # Infinite grid relative to player
            grid_spacing = 40
            offset_x = int(cam_center_x - zone_local_x * 4) % grid_spacing
            offset_y = int(cam_center_y - zone_local_y * 4) % grid_spacing

            for gx in range(offset_x - grid_spacing, arena_px_w + grid_spacing, grid_spacing):
                pygame.draw.line(screen, (28, 44, 38), (gx, 0), (gx, arena_px_h), 1)
            for gy in range(offset_y - grid_spacing, arena_px_h + grid_spacing, grid_spacing):
                pygame.draw.line(screen, (28, 44, 38), (0, gy), (arena_px_w, gy), 1)

            # Draw Dungeon Portal at (0, 0)
            portal_screen_x = int(cam_center_x + (0 - zone_local_x) * 4)
            portal_screen_y = int(cam_center_y + (0 - zone_local_y) * 4)

            pulse = 0.5 + 0.5 * math.sin(now * 3.0)
            portal_rad = int(28 + 6 * pulse)
            portal_surf = pygame.Surface((portal_rad * 2, portal_rad * 2), pygame.SRCALPHA)
            pygame.draw.circle(portal_surf, (80, 160, 255, 60), (portal_rad, portal_rad), portal_rad)
            pygame.draw.circle(portal_surf, (120, 200, 255, 200), (portal_rad, portal_rad), portal_rad - 4, 2)
            screen.blit(portal_surf, (portal_screen_x - portal_rad, portal_screen_y - portal_rad))

            portal_lbl = font_small.render(f"DUNGEON ROOM #{room_id} [J / Enter]", True, (160, 220, 255))
            screen.blit(portal_lbl, (portal_screen_x - portal_lbl.get_width() // 2, portal_screen_y - portal_rad - 16))

            # Draw local player at screen center
            pygame.draw.ellipse(screen, (10, 18, 14), (cam_center_x - 14, cam_center_y + 9, 28, 8))
            pygame.draw.circle(screen, (50, 210, 120), (cam_center_x, cam_center_y), 14)
            pygame.draw.circle(screen, (255, 255, 255), (cam_center_x, cam_center_y), 17, 2)
            my_lbl = font_small.render(f"P{player_id} (YOU)", True, (255, 255, 255))
            screen.blit(my_lbl, (cam_center_x - my_lbl.get_width() // 2, cam_center_y - 32))

            # Draw other visible players in Zone on the field
            for peer_id in world.zone_visible_players:
                peer_x, peer_y = world.get_zone_peer_pos(peer_id, now)
                peer_scr_x = int(cam_center_x + (peer_x - zone_local_x) * 4)
                peer_scr_y = int(cam_center_y + (peer_y - zone_local_y) * 4)
                dist = int(math.hypot(peer_x - zone_local_x, peer_y - zone_local_y))

                # Check if peer is inside visible viewport
                if 0 <= peer_scr_x <= arena_px_w and 0 <= peer_scr_y <= arena_px_h:
                    # Ground shadow
                    pygame.draw.ellipse(screen, (10, 18, 22), (peer_scr_x - 14, peer_scr_y + 9, 28, 8))

                    # Pulsing aura glow
                    pulse = 0.5 + 0.5 * math.sin(now * 3.0 + peer_id)
                    glow_r = int(16 + 2 * pulse)
                    glow_surf = pygame.Surface((glow_r * 2, glow_r * 2), pygame.SRCALPHA)
                    pygame.draw.circle(glow_surf, (80, 160, 255, 60), (glow_r, glow_r), glow_r)
                    screen.blit(glow_surf, (peer_scr_x - glow_r, peer_scr_y - glow_r))

                    # Avatar body and border
                    pygame.draw.circle(screen, (60, 160, 240), (peer_scr_x, peer_scr_y), 14)
                    pygame.draw.circle(screen, (160, 210, 255), (peer_scr_x, peer_scr_y), 16, 2)

                    # Label pill above avatar
                    peer_lbl = font_small.render(f"P{peer_id}", True, (220, 240, 255))
                    lbl_bg = pygame.Surface((peer_lbl.get_width() + 8, peer_lbl.get_height() + 2), pygame.SRCALPHA)
                    lbl_bg.fill((20, 30, 42, 190))
                    screen.blit(lbl_bg, (peer_scr_x - peer_lbl.get_width() // 2 - 4, peer_scr_y - 32))
                    screen.blit(peer_lbl, (peer_scr_x - peer_lbl.get_width() // 2, peer_scr_y - 31))

                    # Distance tag below avatar
                    dist_lbl = font_small.render(f"{dist}m", True, (130, 180, 220))
                    screen.blit(dist_lbl, (peer_scr_x - dist_lbl.get_width() // 2, peer_scr_y + 18))
                else:
                    # Off-screen radar indicator along the boundary
                    dx_off = peer_scr_x - cam_center_x
                    dy_off = peer_scr_y - cam_center_y
                    angle = math.atan2(dy_off, dx_off)
                    margin = 25
                    edge_x = max(margin, min(arena_px_w - margin, cam_center_x + math.cos(angle) * (arena_px_w // 2 - margin)))
                    edge_y = max(margin, min(arena_px_h - margin, cam_center_y + math.sin(angle) * (arena_px_h // 2 - margin)))

                    pygame.draw.circle(screen, (60, 160, 240), (int(edge_x), int(edge_y)), 7)
                    pygame.draw.circle(screen, (200, 230, 255), (int(edge_x), int(edge_y)), 9, 1)
                    tag = font_small.render(f"P{peer_id} ({dist}m)", True, (160, 210, 255))
                    screen.blit(tag, (int(edge_x) - tag.get_width() // 2, int(edge_y) - 18))

            # Banner on top of Zone screen
            zone_badge = font_large.render(f"OPEN FIELD: ZONE #{world.zone_id}", True, (100, 230, 160))
            screen.blit(zone_badge, (20, 15))

            # Side Panel in Zone Mode
            panel_x = arena_px_w
            panel_rect = pygame.Rect(panel_x, 0, panel_w, win_h)
            pygame.draw.rect(screen, (24, 30, 36), panel_rect)
            pygame.draw.line(screen, (60, 90, 80), (panel_x, 0), (panel_x, win_h), 2)

            pad_x = panel_x + 15
            py_offset = 15

            screen.blit(font_large.render("ZONE EXPLORE", True, (240, 245, 255)), (pad_x, py_offset))
            py_offset += 26
            screen.blit(font_small.render(f"Player #{player_id}  |  Zone #{world.zone_id}", True, (150, 175, 170)), (pad_x, py_offset))
            py_offset += 22

            pygame.draw.line(screen, (40, 60, 55), (pad_x, py_offset), (panel_x + panel_w - 15, py_offset), 1)
            py_offset += 12

            screen.blit(font_small.render("CURRENT LOCATION", True, (150, 175, 170)), (pad_x, py_offset))
            py_offset += 16
            coord_text = font_medium.render(f"X: {int(zone_local_x)}   Y: {int(zone_local_y)}", True, (100, 230, 160))
            screen.blit(coord_text, (pad_x, py_offset))
            py_offset += 26

            screen.blit(font_small.render(f"VISIBLE PEERS ({len(world.zone_visible_players)})", True, (150, 175, 170)), (pad_x, py_offset))
            py_offset += 16
            if world.zone_visible_players:
                for peer in world.zone_visible_players[:6]:
                    peer_x, peer_y = world.get_zone_peer_pos(peer, now)
                    dist = int(math.hypot(peer_x - zone_local_x, peer_y - zone_local_y))
                    screen.blit(font_small.render(f"• Player #{peer} ({dist}m)", True, (180, 210, 240)), (pad_x + 5, py_offset))
                    py_offset += 16
            else:
                screen.blit(font_small.render("No other players nearby", True, (100, 120, 115)), (pad_x + 5, py_offset))
                py_offset += 18

            py_offset += 10
            pygame.draw.line(screen, (40, 60, 55), (pad_x, py_offset), (panel_x + panel_w - 15, py_offset), 1)
            py_offset += 12

            # Dungeon entry prompt
            screen.blit(font_small.render("BATTLE DUNGEON", True, (150, 175, 170)), (pad_x, py_offset))
            py_offset += 16
            dungeon_btn_rect = pygame.Rect(pad_x, py_offset, 210, 36)
            pygame.draw.rect(screen, (40, 90, 130), dungeon_btn_rect)
            pygame.draw.rect(screen, (80, 180, 255), dungeon_btn_rect, 1)
            btn_txt = font_medium.render(f"ENTER ROOM #{room_id}", True, (255, 255, 255))
            screen.blit(btn_txt, (pad_x + 105 - btn_txt.get_width() // 2, py_offset + 9))
            py_offset += 46
            screen.blit(font_small.render("Press [J] or [ENTER] to join", True, (160, 200, 230)), (pad_x + 10, py_offset))

            # Controls guide at bottom
            ctrl_y = win_h - 55
            pygame.draw.line(screen, (40, 60, 55), (pad_x, ctrl_y - 8), (panel_x + panel_w - 15, ctrl_y - 8), 1)
            screen.blit(font_small.render("WASD/Arrows: Move in Zone", True, (140, 170, 160)), (pad_x, ctrl_y))
            screen.blit(font_small.render("J/ENTER: Enter Room | ESC: Quit", True, (140, 170, 160)), (pad_x, ctrl_y + 16))

        # ==========================
        # VIEW 2: ROOM ARENA MODE
        # ==========================
        else:
            # 1. Arena rendering
            arena_rect = pygame.Rect(0, 0, arena_px_w, arena_px_h)
            pygame.draw.rect(screen, (24, 27, 38), arena_rect)

            # Grid lines (every 10 units)
            grid_step = 10 * scale
            for gx in range(grid_step, arena_px_w, grid_step):
                pygame.draw.line(screen, (34, 38, 54), (gx, 0), (gx, arena_px_h), 1)
            for gy in range(grid_step, arena_px_h, grid_step):
                pygame.draw.line(screen, (34, 38, 54), (0, gy), (arena_px_w, gy), 1)

            # Arena border
            pygame.draw.rect(screen, (70, 80, 110), arena_rect, 2)

            # Range circle around local player
            my_entity = world.players.get(player_id)
            if my_entity and my_entity.alive:
                my_x, my_y = my_entity.interpolated_pos(now)
                center_x = int(my_x * scale)
                center_y = int(my_y * scale)
                range_px = slash_range * scale
                range_surf = pygame.Surface((range_px * 2, range_px * 2), pygame.SRCALPHA)
                pygame.draw.circle(range_surf, (60, 220, 120, 35), (range_px, range_px), range_px)
                pygame.draw.circle(range_surf, (60, 220, 120, 90), (range_px, range_px), range_px, 1)
                screen.blit(range_surf, (center_x - range_px, center_y - range_px))

            # Render Enemies
            for enemy in world.enemies.values():
                ex, ey = enemy.interpolated_pos(now)
                scr_x = int(ex * scale)
                scr_y = int(ey * scale)

                if enemy.kind == EnemyKind.Boss:
                    radius = max(18, 4 * scale)
                    pygame.draw.circle(screen, (220, 45, 55), (scr_x, scr_y), radius)
                    pygame.draw.circle(screen, (255, 120, 120), (scr_x, scr_y), radius, 2)
                    boss_txt = font_small.render("BOSS", True, (255, 255, 255))
                    screen.blit(boss_txt, (scr_x - boss_txt.get_width() // 2, scr_y - boss_txt.get_height() // 2))

                    hp_bar_w = 48
                    hp_bar_h = 6
                    hp_bar_x = scr_x - hp_bar_w // 2
                    hp_bar_y = scr_y - radius - 10
                    pygame.draw.rect(screen, (50, 50, 50), (hp_bar_x, hp_bar_y, hp_bar_w, hp_bar_h))
                    ratio = max(0.0, min(1.0, enemy.hp / max(1, enemy.max_hp)))
                    pygame.draw.rect(screen, (220, 45, 55), (hp_bar_x, hp_bar_y, int(hp_bar_w * ratio), hp_bar_h))
                    pygame.draw.rect(screen, (100, 100, 100), (hp_bar_x, hp_bar_y, hp_bar_w, hp_bar_h), 1)
                else:
                    radius = max(6, int(1.8 * scale))
                    pygame.draw.circle(screen, (235, 100, 60), (scr_x, scr_y), radius)
                    pygame.draw.circle(screen, (255, 170, 130), (scr_x, scr_y), radius, 1)

                    hp_bar_w = 24
                    hp_bar_h = 4
                    hp_bar_x = scr_x - hp_bar_w // 2
                    hp_bar_y = scr_y - radius - 7
                    pygame.draw.rect(screen, (50, 50, 50), (hp_bar_x, hp_bar_y, hp_bar_w, hp_bar_h))
                    ratio = max(0.0, min(1.0, enemy.hp / max(1, enemy.max_hp)))
                    pygame.draw.rect(screen, (235, 100, 60), (hp_bar_x, hp_bar_y, int(hp_bar_w * ratio), hp_bar_h))

            # Render Players
            for player in world.players.values():
                px, py = player.interpolated_pos(now)
                scr_x = int(px * scale)
                scr_y = int(py * scale)
                radius = max(8, int(2.2 * scale))

                is_self = (player.id == player_id)

                if not player.alive:
                    pygame.draw.circle(screen, (70, 70, 80), (scr_x, scr_y), radius)
                    pygame.draw.line(screen, (200, 40, 40), (scr_x - 6, scr_y - 6), (scr_x + 6, scr_y + 6), 2)
                    pygame.draw.line(screen, (200, 40, 40), (scr_x + 6, scr_y - 6), (scr_x - 6, scr_y + 6), 2)
                    dead_txt = font_small.render("DEAD", True, (200, 80, 80))
                    screen.blit(dead_txt, (scr_x - dead_txt.get_width() // 2, scr_y + radius + 2))
                else:
                    color = (50, 210, 120) if is_self else (60, 160, 240)
                    pygame.draw.circle(screen, color, (scr_x, scr_y), radius)

                    if is_self:
                        pygame.draw.circle(screen, (255, 255, 255), (scr_x, scr_y), radius + 2, 2)

                    label_text = f"P{player.id} (YOU)" if is_self else f"P{player.id}"
                    label_color = (255, 255, 255) if is_self else (180, 200, 230)
                    lbl = font_small.render(label_text, True, label_color)
                    screen.blit(lbl, (scr_x - lbl.get_width() // 2, scr_y - radius - 18))

                    hp_bar_w = 28
                    hp_bar_h = 4
                    hp_bar_x = scr_x - hp_bar_w // 2
                    hp_bar_y = scr_y - radius - 6
                    pygame.draw.rect(screen, (40, 40, 40), (hp_bar_x, hp_bar_y, hp_bar_w, hp_bar_h))
                    ratio = max(0.0, min(1.0, player.hp / max(1, player.max_hp)))
                    hp_color = (50, 210, 120) if ratio > 0.4 else (220, 50, 50)
                    pygame.draw.rect(screen, hp_color, (hp_bar_x, hp_bar_y, int(hp_bar_w * ratio), hp_bar_h))

            # 2. Side Panel rendering
            panel_x = arena_px_w
            panel_rect = pygame.Rect(panel_x, 0, panel_w, win_h)
            pygame.draw.rect(screen, (26, 30, 42), panel_rect)
            pygame.draw.line(screen, (70, 80, 110), (panel_x, 0), (panel_x, win_h), 2)

            py_offset = 15
            pad_x = panel_x + 15

            title_surf = font_large.render("SnF BATTLE", True, (240, 245, 255))
            screen.blit(title_surf, (pad_x, py_offset))
            py_offset += 26

            info_surf = font_small.render(f"Player #{player_id}  |  Room #{room_id}", True, (160, 170, 195))
            screen.blit(info_surf, (pad_x, py_offset))
            py_offset += 20

            phase_colors = {
                RoomPhase.Waiting: (240, 180, 50),
                RoomPhase.Running: (50, 210, 120),
                RoomPhase.Cleared: (80, 220, 255),
                RoomPhase.Failed: (240, 60, 60),
            }
            phase_color = phase_colors.get(world.phase, (200, 200, 200))
            phase_text = font_medium.render(f"Phase: {world.phase.name}", True, phase_color)
            screen.blit(phase_text, (pad_x, py_offset))
            py_offset += 28

            pygame.draw.line(screen, (45, 52, 72), (pad_x, py_offset), (panel_x + panel_w - 15, py_offset), 1)
            py_offset += 10

            screen.blit(font_small.render("MY HEALTH", True, (160, 170, 195)), (pad_x, py_offset))
            py_offset += 16
            bar_w = 210
            bar_h = 16
            pygame.draw.rect(screen, (40, 45, 60), (pad_x, py_offset, bar_w, bar_h))
            my_hp = my_entity.hp if my_entity else 0
            my_max_hp = my_entity.max_hp if my_entity else 100
            my_ratio = max(0.0, min(1.0, my_hp / max(1, my_max_hp)))
            pygame.draw.rect(screen, (50, 210, 120), (pad_x, py_offset, int(bar_w * my_ratio), bar_h))
            pygame.draw.rect(screen, (70, 80, 110), (pad_x, py_offset, bar_w, bar_h), 1)
            my_hp_text = font_small.render(f"{my_hp} / {my_max_hp}", True, (255, 255, 255))
            screen.blit(my_hp_text, (pad_x + bar_w // 2 - my_hp_text.get_width() // 2, py_offset + 1))
            py_offset += 24

            screen.blit(font_small.render("BOSS HEALTH", True, (160, 170, 195)), (pad_x, py_offset))
            py_offset += 16
            pygame.draw.rect(screen, (40, 45, 60), (pad_x, py_offset, bar_w, bar_h))
            boss_entity = world.enemies.get(world.boss_id) if world.boss_id else None
            if boss_entity:
                boss_ratio = max(0.0, min(1.0, boss_entity.hp / max(1, boss_entity.max_hp)))
                pygame.draw.rect(screen, (220, 50, 60), (pad_x, py_offset, int(bar_w * boss_ratio), bar_h))
                b_txt = f"{boss_entity.hp} / {boss_entity.max_hp}"
            elif world.boss_final_hp is not None:
                b_txt = f"Final HP: {world.boss_final_hp}"
            else:
                b_txt = "Not Spawned"
            pygame.draw.rect(screen, (70, 80, 110), (pad_x, py_offset, bar_w, bar_h), 1)
            b_surf = font_small.render(b_txt, True, (255, 255, 255))
            screen.blit(b_surf, (pad_x + bar_w // 2 - b_surf.get_width() // 2, py_offset + 1))
            py_offset += 24

            screen.blit(font_small.render("SLASH SKILL [SPACE]", True, (160, 170, 195)), (pad_x, py_offset))
            py_offset += 16
            cd_elapsed = now - last_skill_time
            cd_ratio = max(0.0, min(1.0, cd_elapsed / slash_cooldown))
            pygame.draw.rect(screen, (40, 45, 60), (pad_x, py_offset, bar_w, bar_h))
            cd_color = (255, 200, 50) if cd_ratio >= 1.0 else (80, 110, 160)
            pygame.draw.rect(screen, cd_color, (pad_x, py_offset, int(bar_w * cd_ratio), bar_h))
            pygame.draw.rect(screen, (70, 80, 110), (pad_x, py_offset, bar_w, bar_h), 1)
            cd_txt_str = "READY (10 DMG, RNG 12)" if cd_ratio >= 1.0 else f"Cooldown {slash_cooldown - cd_elapsed:.1f}s"
            cd_surf = font_small.render(cd_txt_str, True, (20, 20, 25) if cd_ratio >= 1.0 else (220, 220, 230))
            screen.blit(cd_surf, (pad_x + bar_w // 2 - cd_surf.get_width() // 2, py_offset + 1))
            py_offset += 26

            pygame.draw.line(screen, (45, 52, 72), (pad_x, py_offset), (panel_x + panel_w - 15, py_offset), 1)
            py_offset += 10

            elapsed = (now - world.battle_start_time) if world.battle_start_time else 0.0
            timer_text = font_small.render(f"Elapsed: {int(elapsed)//60:02d}:{int(elapsed)%60:02d}", True, (200, 210, 230))
            screen.blit(timer_text, (pad_x, py_offset))
            py_offset += 16

            boss_countdown = max(0.0, boss_seconds - elapsed)
            boss_timer_text = font_small.render(f"Boss Spawn: {boss_countdown:.1f}s (assumed)", True, (160, 170, 195))
            screen.blit(boss_timer_text, (pad_x, py_offset))
            py_offset += 16

            battle_countdown = max(0.0, battle_seconds - elapsed)
            deadline_text = font_small.render(f"Deadline: {battle_countdown:.1f}s (assumed)", True, (160, 170, 195))
            screen.blit(deadline_text, (pad_x, py_offset))
            py_offset += 24

            pygame.draw.line(screen, (45, 52, 72), (pad_x, py_offset), (panel_x + panel_w - 15, py_offset), 1)
            py_offset += 10
            screen.blit(font_small.render("COMBAT LOG", True, (160, 170, 195)), (pad_x, py_offset))
            py_offset += 16

            log_box = pygame.Rect(pad_x, py_offset, bar_w, 140)
            pygame.draw.rect(screen, (18, 20, 28), log_box)
            pygame.draw.rect(screen, (45, 52, 72), log_box, 1)

            log_y = py_offset + 6
            for log_entry in list(world.log)[-8:]:
                entry_surf = font_small.render(log_entry, True, (190, 200, 220))
                if entry_surf.get_width() > bar_w - 10:
                    entry_surf = font_small.render(log_entry[:28] + "...", True, (190, 200, 220))
                screen.blit(entry_surf, (pad_x + 5, log_y))
                log_y += 16

            ctrl_y = win_h - 55
            pygame.draw.line(screen, (45, 52, 72), (pad_x, ctrl_y - 8), (panel_x + panel_w - 15, ctrl_y - 8), 1)
            screen.blit(font_small.render("WASD/Arrows: Move | SPACE: Slash", True, (130, 140, 165)), (pad_x, ctrl_y))
            screen.blit(font_small.render("R: Battle Start | ESC: Return to Zone", True, (130, 140, 165)), (pad_x, ctrl_y + 16))

            # 3. Game Over Overlay
            if world.phase in (RoomPhase.Cleared, RoomPhase.Failed):
                overlay = pygame.Surface((arena_px_w, arena_px_h), pygame.SRCALPHA)
                overlay.fill((0, 0, 0, 160))
                screen.blit(overlay, (0, 0))

                if world.phase == RoomPhase.Cleared:
                    txt = font_banner.render("VICTORY!", True, (80, 255, 120))
                    sub = font_medium.render(f"Experience Gained: +{world.cleared_exp or 0}", True, (240, 240, 240))
                else:
                    txt = font_banner.render("DEFEATED", True, (255, 60, 60))
                    sub = font_medium.render(f"Reason: {world.failure_reason or 'Failed'}", True, (240, 240, 240))

                screen.blit(txt, (arena_px_w // 2 - txt.get_width() // 2, arena_px_h // 2 - 40))
                screen.blit(sub, (arena_px_w // 2 - sub.get_width() // 2, arena_px_h // 2 + 10))

                ret_info = font_small.render("Press ESC to return to Zone field", True, (180, 200, 240))
                screen.blit(ret_info, (arena_px_w // 2 - ret_info.get_width() // 2, arena_px_h // 2 + 35))

        pygame.display.flip()
        clock.tick(60)

    if session.in_room:
        session.send_room_leave()
    session.close()
    for b in bot_list:
        b.stop()
    pygame.quit()


def main() -> None:
    parser = argparse.ArgumentParser(description="SnF Minimal Battle Client (Python + pygame)")
    parser.add_argument("--host", default="127.0.0.1", help="Server host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=7777, help="Server port (default: 7777)")
    parser.add_argument("--player", type=int, default=1, help="Player ID (default: 1)")
    parser.add_argument("--zone", type=int, default=1, help="Zone ID (default: 1)")
    parser.add_argument("--room", type=int, default=1, help="Room ID (default: 1)")
    parser.add_argument("--start", action="store_true", help="Start battle immediately upon join")
    parser.add_argument("--headless", action="store_true", help="Run in headless text mode without GUI")
    parser.add_argument("--zone-first", action="store_true", help="Start in Zone open field first (press J/Enter to join room)")
    parser.add_argument("--bots", type=int, default=0, help="Number of AI bot teammates to spawn in the room (default: 0)")
    parser.add_argument("--scale", type=int, default=6, help="Scale factor for arena rendering (default: 6)")
    parser.add_argument("--boss-seconds", type=float, default=40.0, help="Assumed boss spawn seconds (default: 40)")
    parser.add_argument("--battle-seconds", type=float, default=90.0, help="Assumed battle deadline seconds (default: 90)")

    args = parser.parse_args()

    if args.headless:
        run_headless(
            host=args.host,
            port=args.port,
            player_id=args.player,
            zone_id=args.zone,
            room_id=args.room,
            start=args.start,
            boss_seconds=args.boss_seconds,
            battle_seconds=args.battle_seconds,
            bots=args.bots,
            zone_first=args.zone_first,
        )
    else:
        run_gui(
            host=args.host,
            port=args.port,
            player_id=args.player,
            zone_id=args.zone,
            room_id=args.room,
            start=args.start,
            scale=args.scale,
            boss_seconds=args.boss_seconds,
            battle_seconds=args.battle_seconds,
            bots=args.bots,
            zone_first=args.zone_first,
        )


if __name__ == "__main__":
    main()
