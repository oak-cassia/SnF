from __future__ import annotations

import queue
import socket
import threading
import time
from typing import Optional

import snf_wire
from snf_wire import Direction, EquipSkillStatus, Frame, FrameDecoder, MessageType, PurchaseStatus, RoomPhase, RoomStatus, ZoneCommandStatus


class Session:
    def __init__(self, host: str = "127.0.0.1", port: int = 7777) -> None:
        self.host = host
        self.port = port
        self.player_id: int = 0
        self.zone_id: int = 0
        self.room_id: int = 0

        self.in_zone: bool = False
        self.in_room: bool = False
        self._move_sequence: int = 1
        self._skill_sequence: int = 1
        self._next_request_id: int = 1

        self._sock: Optional[socket.socket] = None
        self._reader_thread: Optional[threading.Thread] = None
        self._queue: queue.Queue[Optional[Frame]] = queue.Queue()
        self._pending_pushes: list[Frame] = []
        self._receive_lock = threading.Lock()
        self._connected: bool = False
        self._close_requested: bool = False
        self._decoder = FrameDecoder()

    @property
    def is_connected(self) -> bool:
        return self._connected

    def connect(self, timeout: float = 5.0) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((self.host, self.port))
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.settimeout(None)

        self._sock = sock
        self._connected = True
        self._close_requested = False

        self._reader_thread = threading.Thread(target=self._read_loop, daemon=True)
        self._reader_thread.start()

    def _read_loop(self) -> None:
        assert self._sock is not None
        while not self._close_requested:
            try:
                data = self._sock.recv(4096)
                if not data:
                    break
                self._decoder.feed(data)
                for frame in self._decoder.frames():
                    self._queue.put(frame)
            except Exception:
                break

        self._connected = False
        self._queue.put(None)

    def _alloc_request_id(self) -> int:
        req_id = self._next_request_id
        self._next_request_id += 1
        if self._next_request_id == 0:
            self._next_request_id = 1
        return req_id

    def _send_frame(self, msg_type: MessageType, request_id: int, payload: bytes = b"") -> None:
        if not self._connected or self._sock is None:
            raise ConnectionError("Socket is not connected")
        data = snf_wire.encode(msg_type, request_id, payload)
        self._sock.sendall(data)

    def request(
        self,
        msg_type: MessageType,
        payload: bytes,
        expect_type: MessageType,
        timeout: float = 5.0,
    ) -> Frame:
        with self._receive_lock:
            req_id = self._alloc_request_id()
            self._send_frame(msg_type, req_id, payload)

            deadline = time.monotonic() + timeout
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"Timed out waiting for response to {msg_type.name} (req_id={req_id})")

                try:
                    frame = self._queue.get(timeout=remaining)
                except queue.Empty:
                    raise TimeoutError(f"Timed out waiting for response to {msg_type.name} (req_id={req_id})")

                if frame is None:
                    raise ConnectionResetError("Server closed the connection during request")

                if frame.request_id == req_id:
                    if frame.type != expect_type:
                        raise RuntimeError(
                            f"Expected {expect_type.name} for request {msg_type.name} (req_id={req_id}), got {frame.type.name}"
                        )
                    return frame

                self._pending_pushes.append(frame)

    def drain_pushes(self) -> list[Frame]:
        if not self._receive_lock.acquire(blocking=False):
            return []

        try:
            results = list(self._pending_pushes)
            self._pending_pushes.clear()

            while True:
                try:
                    frame = self._queue.get_nowait()
                except queue.Empty:
                    break

                if frame is None:
                    self._connected = False
                    break

                results.append(frame)

            for frame in results:
                if frame.type in (MessageType.BattleCleared, MessageType.BattleFailed):
                    self.in_room = False
                elif frame.type == MessageType.ReturnedToZone:
                    self.in_room = False
                    self.in_zone = True

            return results
        finally:
            self._receive_lock.release()

    def bootstrap_zone(
        self,
        player_id: int,
        zone_id: int,
        x: int = 0,
        y: int = 0,
    ) -> dict:
        self.player_id = player_id
        self.zone_id = zone_id

        auth_resp = self.request(
            MessageType.Authenticate,
            snf_wire.authenticate(player_id),
            MessageType.Authenticated,
        )
        auth_pid = snf_wire.parse_authenticated(auth_resp.payload)
        if auth_pid != player_id:
            raise RuntimeError(f"Authenticated player_id mismatch: expected {player_id}, got {auth_pid}")

        zone_resp = self.request(
            MessageType.EnterZone,
            snf_wire.enter_zone(zone_id, x, y),
            MessageType.ZoneEntered,
        )
        zone_data = snf_wire.parse_zone_entered(zone_resp.payload)
        if zone_data["status"] != 0:
            raise RuntimeError(f"EnterZone failed with status {zone_data['status']}")

        self.in_zone = True
        self.in_room = False
        return zone_data

    def join_room(self, room_id: int | None = None, start: bool = False) -> tuple[RoomStatus, RoomPhase]:
        if room_id is not None:
            self.room_id = room_id

        join_resp = self.request(
            MessageType.RoomJoin,
            snf_wire.room_join(self.room_id),
            MessageType.RoomJoined,
        )
        status, phase, resp_room = snf_wire.parse_room_reply(join_resp.payload)
        if status != RoomStatus.Applied:
            raise RuntimeError(
                f"RoomJoin rejected: status={status.name} ({status.value}), phase={phase.name} ({phase.value})"
            )
        if resp_room != self.room_id:
            raise RuntimeError(f"RoomJoin room mismatch: expected {self.room_id}, got {resp_room}")

        self.in_room = True
        self.in_zone = False

        if start:
            start_resp = self.request(
                MessageType.BattleStart,
                snf_wire.battle_start(self.room_id),
                MessageType.BattleStarted,
            )
            s_status, s_phase, s_room = snf_wire.parse_room_reply(start_resp.payload)
            if s_status != RoomStatus.Applied:
                raise RuntimeError(
                    f"BattleStart rejected: status={s_status.name} ({s_status.value}), phase={s_phase.name} ({s_phase.value})"
                )
            phase = s_phase

        return status, phase

    def purchase(self, idempotency_key: int, product_id: int) -> dict:
        response = self.request(
            MessageType.Purchase,
            snf_wire.purchase(idempotency_key, product_id),
            MessageType.PurchaseResult,
        )
        result = snf_wire.parse_purchase_result(response.payload)
        if result["status"] not in (PurchaseStatus.Committed, PurchaseStatus.AlreadyOwned):
            raise RuntimeError(f"Purchase rejected: status={result['status'].name} ({result['status'].value})")
        return result

    def equip_skill(self, skill_id: int) -> tuple[EquipSkillStatus, int]:
        response = self.request(
            MessageType.EquipSkill,
            snf_wire.equip_skill(skill_id),
            MessageType.EquipSkillResult,
        )
        status, equipped_skill_id = snf_wire.parse_equip_skill_result(response.payload)
        if status not in (EquipSkillStatus.Equipped, EquipSkillStatus.AlreadyEquipped):
            raise RuntimeError(f"EquipSkill rejected: status={status.name} ({status.value})")
        return status, equipped_skill_id

    def bootstrap(
        self,
        player_id: int,
        zone_id: int,
        room_id: int,
        x: int = 0,
        y: int = 0,
        start: bool = False,
        auto_join_room: bool = True,
    ) -> None:
        self.room_id = room_id
        self.bootstrap_zone(player_id=player_id, zone_id=zone_id, x=x, y=y)

        if auto_join_room:
            self.join_room(room_id=room_id, start=start)

    def send_zone_move(self, x: int, y: int) -> bool:
        if not self.in_zone or self.in_room:
            return False
        req_id = self._alloc_request_id()
        payload = snf_wire.move_in_zone(x, y)
        self._send_frame(MessageType.Move, req_id, payload)
        return True

    def send_move_intent(self, direction: int | Direction) -> bool:
        if not self.in_room:
            return False
        seq = self._move_sequence
        self._move_sequence += 1
        req_id = self._alloc_request_id()
        payload = snf_wire.set_move_intent(self.room_id, direction, seq)
        self._send_frame(MessageType.SetMoveIntent, req_id, payload)
        return True

    def send_use_skill(self, skill_id: int = 1) -> bool:
        if not self.in_room:
            return False
        seq = self._skill_sequence
        self._skill_sequence += 1
        req_id = self._alloc_request_id()
        payload = snf_wire.use_skill(self.room_id, skill_id, seq)
        self._send_frame(MessageType.UseSkill, req_id, payload)
        return True

    def send_battle_start(self) -> bool:
        if not self.in_room:
            return False
        req_id = self._alloc_request_id()
        payload = snf_wire.battle_start(self.room_id)
        self._send_frame(MessageType.BattleStart, req_id, payload)
        return True

    def send_room_leave(self) -> bool:
        if not self.in_room:
            return False
        req_id = self._alloc_request_id()
        payload = snf_wire.room_leave()
        self._send_frame(MessageType.RoomLeave, req_id, payload)
        self.in_room = False
        return True

    def close(self) -> None:
        self._close_requested = True
        self._connected = False
        if self._sock is not None:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None
        if self._reader_thread is not None and self._reader_thread.is_alive():
            self._reader_thread.join(timeout=1.0)
