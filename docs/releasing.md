# Preview, RC와 정식 배포

NATS는 다른 Spine 플랫폼과 같은 `preview → rc → main` 절차를 사용합니다. 초기 `rc`와 `main`은 기존 소스의 기준 브랜치이며, 브랜치 생성 자체는 RC나 정식 버전 발행이 아닙니다. 기본 브랜치는 `preview`입니다.

| 단계 | 브랜치 | 태그 | 다음 단계 조건 |
| --- | --- | --- | --- |
| Preview | `preview` | `v0.1.0-preview.N` | 공개 검증과 72시간 관찰 |
| RC | `rc` | `v0.1.0-rc.N` | 공개 검증과 24시간 관찰 |
| 정식 | `main` | `v0.1.0` | 지정 리뷰어 승인, 정식 발행 간격 168시간 |

Preview와 RC는 태그와 `release-manifest-<tag>` Actions artifact를 남깁니다. GitHub Release는 정식 버전만 만듭니다. 관찰 시간을 줄여야 할 때는 워크플로 입력에 20자 이상의 사유를 기록합니다.

## 변경과 승격

작업 PR은 `preview`를 대상으로 합니다. 채널 승격은 `preview → rc`, `rc → main`의 merge commit을 사용합니다. 의존성 고정 버전만 바꿀 때는 같은 저장소의 `promote/rc/v0.1.0-rc.N`, `promote/stable/v0.1.0` 브랜치도 허용합니다. 코드를 바꿨다면 새 Preview부터 시작합니다.

코어의 채널 태그가 공개된 후 승격 브랜치에서 변경을 검토하고 적용합니다.

```sh
python3 scripts/release/pin_channel.py --channel rc
python3 scripts/release/pin_channel.py --channel rc --apply --tidy
python3 scripts/release/release.py check-module --channel rc
```

정식 승격에는 `--channel stable`을 사용합니다. 루트, `e2e`와 worker의 코어 의존성을 함께 고정합니다. 채널과 기본 버전 이외의 소스 차이, 검증 출처나 manifest 불일치, 중복 또는 만료된 artifact는 승격을 중단합니다.

## 발행 워크플로

- `Publish Preview`: `preview`에서 새 버전과 현재 HEAD의 전체 SHA를 지정합니다.
- `Publish Rc`: `rc`에서 새 RC 버전과 검증한 Preview 태그를 지정합니다.
- `Publish Stable`: `main`에서 검증한 RC 태그를 지정하고 `release-stable` 승인을 받습니다.

발행 전후에 별도 캐시로 공개 모듈 설치와 consumer build, 실제 JetStream 송수신, TLS/인증과 앱 E2E를 검사합니다. 검사가 실패하면 발행 완료 manifest를 만들지 않으며 기존 태그를 이동하거나 재사용하지 않습니다.

일반 소스 CI는 정책의 `ci_core` 전체 SHA와 별도 workspace를 사용합니다. 생성 프로젝트 E2E는 같은 코어 소스에 검증 채널의 이름을 붙인 별도 프록시를 사용해 Preview 전용 CLI의 버전 조회를 연결합니다. 이 소스 경로는 공개 설치 증거가 아닙니다. 발행용 CI는 실제 공개 코어 버전을 사용하고 공개 검증에서는 `go.work`와 로컬 `replace`를 허용하지 않습니다. 미등록 이벤트와 설정형 validator를 포함한 생성 앱 여덟 조합과 실제 브로커 검사의 누락과 test skip은 실패로 처리합니다.

## 저장소 설정

초기 채널을 준비한 뒤 다음 명령으로 설정을 적용하고 재검사합니다.

```sh
python3 scripts/release/configure_branches.py --bootstrap
python3 scripts/release/configure_branches.py --bootstrap --apply
python3 scripts/release/configure_branches.py --apply
python3 scripts/release/configure_branches.py
```

기존 채널 브랜치를 이동하지 않습니다. 세 브랜치의 삭제와 force push를 막고, 채널별 `Branch Flow`와 `Verified Source`를 필수로 요구합니다. `v*` 태그의 수정과 삭제도 막습니다. `release-preview`, `release-rc`, `release-stable`은 해당 브랜치에서만 실행하며 정식 발행은 `NARUBROWN`의 승인을 요구합니다.

## Discord 태그 알림

태그 생성에 성공하면 바로 [Spine 공통 Discord Action](https://github.com/NARUBROWN/spine/tree/4e53e0b5ba0bed84e3eadc9b49bc31bddcc64f4d/.github/actions/discord-tag)을 호출합니다. 알림 제목에 저장소 이름과 태그를 표시하고 태그 페이지와 발행 실행 기록으로 연결합니다. 이 알림은 태그 생성 시점의 기록이며 공개 설치 검증과 Release 게시 완료를 보장하지 않습니다.

| 태그 형식 | GitHub Actions repository secret |
| --- | --- |
| `vX.Y.Z-preview.N` | `DISCORD_WEBHOOK_PREVIEW` |
| `vX.Y.Z-rc.N` | `DISCORD_WEBHOOK_RC` |
| `vX.Y.Z` | `DISCORD_WEBHOOK_RELEASE` |

각 Discord 채널의 webhook URL을 이 저장소의 **Settings → Secrets and variables → Actions**에 위 이름으로 등록합니다. URL은 코드나 로그에 넣지 않습니다. 공통 Action은 전체 커밋 SHA로 고정하며 호출 workflow는 위 세 secret만 전달합니다.

`GITHUB_TOKEN`으로 만든 태그의 후속 이벤트에 의존하지 않고 기존 발행 workflow에서 직접 전송합니다. secret 누락과 전송 오류는 경고를 남기고 발행 검증을 계속합니다. 자동 재시도는 하지 않으며 응답 유실 시 채널 수신 여부를 먼저 확인합니다. 수동 태그 push와 GitHub Release 이벤트는 별도 알림으로 연결하지 않습니다.
