# Repository work instructions

## Unified Worker Runtime 11단계

- 11단계를 구현하거나 리뷰하기 전에
  [`docs/stage-11-workflow-migration-plan.md`](docs/stage-11-workflow-migration-plan.md)를 처음부터 읽는다.
- 이 문서를 11A~11K의 최종 상세 실행 기준으로 사용한다. 완료된 10단계의 기준과 측정값은
  [`docs/stage-10-quality-gate-plan.md`](docs/stage-10-quality-gate-plan.md)와
  [`docs/worker-runtime-quality-gates.md`](docs/worker-runtime-quality-gates.md)에 있다.
- 상위 단계와 완료 조건은
  [`docs/development-roadmap.md`](docs/development-roadmap.md), 런타임 불변식은
  [`docs/architecture/unified-worker-runtime.md`](docs/architecture/unified-worker-runtime.md)를 따른다.
- 구현 중 계획의 전제가 맞지 않음을 발견하면 조용히 우회하지 않는다. 차이를 먼저 알리고 합의된 변경을
  코드, 최종 계획과 로드맵에 함께 반영한다.
- 11단계 체크박스는 해당 테스트 증거가 확보된 뒤에만 닫는다. production 경로의 품질 게이트 재실행 결과는
  계획의 11I에서 `docs/worker-runtime-quality-gates.md`에 추가한다.
- 계획의 "진행 상황" 표를 스텝 완료마다 갱신한다.
