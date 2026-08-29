# SnF 문서 안내

## 아키텍처 기준

- [Unified Worker Runtime 아키텍처](./unified-worker-runtime.md)가 앞으로 구현할 서버 런타임의 유일한
  기준 문서다.
- 루트 [README](../README.md)의 구조 설명과 코드 링크는 전환이 끝날 때까지 **현행 구현**을 설명한다.
  목표 구조로 읽지 않는다.
- 새 문서에서 `ActorRuntime`, `ActorBinding`, 별도 network Reactor, shared `OutboundChannel`,
  `PlayerPersistenceService`, `OutcomeHandler` 계층을 목표 구조로 다시 도입하지 않는다.

## 보존하는 계약과 기록

| 문서 | 분류 | 적용 방식 |
| --- | --- | --- |
| [Room 입장 Handoff](./room-entry-handoff-contract.md) | gameplay 전이 계약 | 입장·실패·보상 의미를 보존하고 실행 주체는 새 Worker ownership에 맞춰 재배치 |
| [Cross-Zone Handoff](./cross-zone-handoff-contract.md) | gameplay 전이 계약 | route 전이 의미를 보존하되 legacy reactor/channel 이름은 구현 기준이 아님 |
| [Player 상태 소유권](./player-state-ownership-contract.md) | domain/persistence 계약 | Player authority는 보존하고 persistence 실행은 Worker-local native async DB가 기본 |
| [Projectile Skill](./projectile-skill-contract.md) | 콘텐츠 계약 | 런타임 전환과 독립적인 게임 규칙 |
| [Room 부하 측정](./room-load-measurement.md) | 역사적 baseline | legacy Reactor/Outbound 구조의 수치이며 새 구조 승인 수치가 아님 |
| [개발 로드맵](./development-roadmap.md) | 전환 순서 | Unified Worker Runtime 전환을 현재 최우선 작업으로 관리 |

문서가 충돌하면 target runtime 구조와 실행 경계는 Unified Worker Runtime 문서가 우선하고, wire와
gameplay 의미는 해당 domain 계약이 우선한다.
