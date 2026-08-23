# SnF 최소 기능 플레이 클라이언트 (Python + pygame)

사람이 키보드로 조작하여 Zone(오픈 필드 탐색) 및 Room(보스 전투)을 시각적으로 확인하고 플레이할 수 있는 독립형 클라이언트입니다. C++ 서버 코드 변경 없이 순수 Python 3.9+로 구현되어 있습니다.

---

## 📁 디렉토리 구성

```
game-client/
├── snf_wire.py      # 프레이밍, MessageType, payload 인코딩/디코딩, Zone/Room 파싱 (I/O 없음)
├── snf_world.py     # Zone 필드 및 Room 전투 상태 관리 (선형 보간, digest 이벤트 처리)
├── snf_session.py   # TCP 소켓 + reader 스레드 + request_id demux + bootstrap + 입력 게이트
├── snf_play.py      # pygame 렌더/입력 루프, Zone/Room 뷰 전환, argparse main, --headless 모드
├── snf_bot.py       # 인공지능 봇 클라이언트 (랜덤 무빙/타겟 추적 + 쿨타임마다 자동 공격)
├── README.md        # 실행 가이드 및 조작키
└── .gitignore       # venv 및 캐시 제외 설정
```

---

## 🚀 환경 준비 및 설치

```bash
# game-client 디렉토리로 이동
cd game-client

# 가상환경 생성 및 pygame 설치 (이미 되어 있다면 생략)
python3 -m venv .venv-play
.venv-play/bin/pip install pygame
```

---

## 🎮 실행 방법

### 1. 🌟 Zone(오픈 필드)에서 먼저 시작하기 (`--zone-first`)
오픈 필드에서 자유롭게 돌아다니다가, 포탈에서 `J` 또는 `Enter`를 눌러 던전(Room)으로 입장합니다.
```bash
.venv-play/bin/python snf_play.py --player 1 --zone 1 --room 1 --zone-first
```
- **필드 이동**: `WASD` / 방향키 (실시간 서버 `Move` 패킷 전송, 주변 플레이어 동기화)
- **주변 플레이어 시각화**: Zone 내에 있는 다른 플레이어/봇이 파란색 아바타 캐릭터(`P2`, `P3`...), 거리 인디케이터, 오프스크린 레이더 비컨으로 필드에 실시간 표시됩니다.
- **던전 입장**: `J` 또는 `Enter` 키로 Room 입장
- **필드 복귀**: Room 전투 중 `ESC` 또는 전투 종료 시 원래 필드 좌표로 복귀

---

### 2. 🤖 Zone 필드에서 AI 봇 동료들과 함께 시작하기 (`--zone-first --bots 3`)
오픈 필드에 AI 봇 3명을 함께 스폰하여 필드 배회 및 실시간 동기화를 확인하고, `J`/`Enter`로 다 같이 던전(Room)에 입장합니다.
```bash
.venv-play/bin/python snf_play.py --player 1 --zone 1 --room 1 --zone-first --bots 3
```

---

### 3. 🤖 4인 파티 한 번에 띄우기 (`--bots 3`)
메인 플레이어 1명과 함께 AI 봇 팀원 3명을 한 방에 자동 생성합니다.
```bash
.venv-play/bin/python snf_play.py --player 1 --zone 1 --room 1 --bots 3 --start
```

---

### 3. 단일 플레이어 빠른 전투 진입 (기본 모드)
```bash
.venv-play/bin/python snf_play.py --player 1 --zone 1 --room 1 --start
```

---

### 4. 프로토콜 검증 (Headless 텍스트 모드)
```bash
python3 snf_play.py --player 1 --zone 1 --room 1 --start --headless
```

---

## ⌨️ 조작키 (Controls)

### 🗺️ Zone(오픈 필드) 모드
| 키 | 동작 | 설명 |
| --- | --- | --- |
| `WASD` / `방향키` | 필드 이동 (`Move`) | 오픈 필드 좌표 이동. 주변 플레이어 목록(`visible_players`) 실시간 갱신 |
| `J` / `Enter` | 던전 입장 (`RoomJoin`) | 해당 번호의 Room 전투 아레나로 진입 |
| `ESC` | 게임 종료 | 클라이언트 종료 |

### ⚔️ Room(전투 아레나) 모드
| 키 | 동작 | 설명 |
| --- | --- | --- |
| `WASD` / `방향키` | 아레나 이동 (`SetMoveIntent`) | 8방향 지속 의도 이동. 키를 떼면 정지(`Stop`) |
| `Space` | 스킬 공격 (`UseSkill`) | SLASH (10 피해, 사거리 12, 1.0초 쿨다운) |
| `R` | 전투 시작 (`BattleStart`) | `Waiting` 단계에서 전투를 시작 |
| `ESC` | 방 퇴장 / 필드 복귀 (`RoomLeave`) | 방을 떠나 원래 Zone 필드 좌표로 복귀 (`ReturnedToZone`) |

---

## 🛡️ 프로토콜 게이트 규칙

1. **`in_room` 입력 게이트**: `RoomJoin` 성공 시 활성화되며, `BattleCleared` / `BattleFailed` 종결 push 또는 퇴장 시 비활성화되어 잔여 이동/스킬 패킷 송신을 차단합니다.
2. **독립 시퀀스 번호**: `move_sequence`와 `skill_sequence`는 각각 1부터 단조 증가하는 독립 카운터를 사용합니다 (0 사용 안 함).
3. **Room ID 매칭**: 항상 현재 접속 중인 `room_id`를 페이로드에 명시합니다.
