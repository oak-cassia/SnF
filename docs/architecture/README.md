# 아키텍처 문서 안내

이 디렉터리는 SnF 서버 런타임의 기준 아키텍처와 그 구현을 설명하는 문서를 보관한다.

| 문서 | 분류 | 역할 |
| --- | --- | --- |
| [Unified Worker Runtime](./unified-worker-runtime.md) | 규범적 기준 문서 | 런타임 구조, ownership, 불변식과 단계별 목표를 정의한다. |
| [Cross-worker Tell과 Publication-safe Quiescence](./cross-worker-tell-and-quiescence.md) | 비규범 구현 해설 | 6단계에서 구현한 cross-worker Actor 전달과 shutdown barrier의 동작 원리를 설명한다. |

구현 해설은 기준 문서를 쉽게 이해하도록 돕지만 새로운 계약을 만들지 않는다. 두 문서가 충돌하면
Unified Worker Runtime을 우선하며, 구현 순서와 남은 작업은 [개발 로드맵](../development-roadmap.md)을
따른다. 전체 문서 분류는 [SnF 문서 안내](../README.md)에서 확인한다.
