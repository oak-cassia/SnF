# Repository work instructions

## Unified Worker Runtime 10단계

- 10단계를 구현하거나 리뷰하기 전에
  [`docs/stage-10-quality-gate-plan.md`](docs/stage-10-quality-gate-plan.md)를 처음부터 읽는다.
- 이 문서를 10A~10H의 최종 상세 실행 기준으로 사용한다. 상위 단계와 완료 조건은
  [`docs/development-roadmap.md`](docs/development-roadmap.md), 런타임 불변식은
  [`docs/architecture/unified-worker-runtime.md`](docs/architecture/unified-worker-runtime.md)를 따른다.
- 구현 중 계획의 전제가 맞지 않음을 발견하면 조용히 우회하지 않는다. 차이를 먼저 알리고 합의된 변경을
  코드, 최종 계획과 로드맵에 함께 반영한다.
- 10단계 체크박스는 해당 테스트와 품질 게이트 증거가 확보된 뒤에만 닫고, 측정 결과는 계획의 10H에 정의된
  `docs/worker-runtime-quality-gates.md`에 기록한다.
