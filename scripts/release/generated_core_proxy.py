#!/usr/bin/env python3
"""Preview 전용 CLI 생성 테스트가 검증 대상 채널의 코어를 사용하도록 연결합니다."""

import argparse
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import zipfile

from ci_workspace import channel_for
from release import CORE, ReleaseError, validate_policy


def write_proxy(download, version, output):
    resolved = download["Version"]
    directory = output / "".join("!" + c.lower() if c.isupper() else c for c in CORE) / "@v"
    directory.mkdir(parents=True, exist_ok=True)
    info = json.loads(Path(download["Info"]).read_text())
    info["Version"] = version
    for name in ("preview.info", version + ".info"):
        (directory / name).write_text(json.dumps(info) + "\n")
    (directory / "list").write_text(version + "\n")
    shutil.copyfile(download["GoMod"], directory / (version + ".mod"))
    destination = directory / (version + ".zip")
    if resolved == version:
        shutil.copyfile(download["Zip"], destination)
    else:
        # 소스 CI에서만 전체 SHA의 모듈 ZIP에 미발행 채널 이름을 붙입니다.
        # 파일 내용은 그대로 두며 이 경로를 공개 설치 증거로 사용하지 않습니다.
        prefix = CORE + "@" + resolved + "/"
        with zipfile.ZipFile(download["Zip"]) as source, zipfile.ZipFile(destination, "w", zipfile.ZIP_DEFLATED) as target:
            for entry in source.infolist():
                if not entry.filename.startswith(prefix):
                    raise ReleaseError("코어 모듈 ZIP의 경로가 올바르지 않습니다")
                target.writestr(CORE + "@" + version + "/" + entry.filename[len(prefix):], source.read(entry))
    return output.resolve().as_uri()


def prepare(policy, ref, output, public=False):
    validate_policy(policy)
    # The CLI resolves its preview recipe dynamically. Use the newest published
    # preview instead of the platform's minimum public-contract dependency.
    listing = subprocess.check_output(["go", "list", "-m", "-versions", CORE], text=True,
                                      env=dict(os.environ, GOWORK="off"))
    previews = [item for item in listing.split()[1:]
                if re.fullmatch(r"v\d+\.\d+\.\d+-preview\.\d+", item)]
    if not previews:
        raise ReleaseError("No published core preview version is available for the CLI integration test")
    version = previews[-1]
    query = version
    result = subprocess.run(["go", "mod", "download", "-json", CORE + "@" + query],
                            env=dict(os.environ, GOWORK="off"), text=True, capture_output=True, check=True)
    download = json.loads(result.stdout)
    if download["Version"] != version:
        raise ReleaseError("공개 코어 버전이 검증 채널과 다릅니다")
    proxy = write_proxy(download, version, output)
    environment = {"GOPROXY": proxy + "," + os.environ.get("GOPROXY", "https://proxy.golang.org,direct")}
    if target := os.environ.get("GITHUB_ENV"):
        with open(target, "a") as stream:
            for key, value in environment.items():
                stream.write(key + "=" + value + "\n")
    return environment


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ref", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--public", action="store_true")
    args = parser.parse_args()
    policy = json.loads(Path(".github/release-policy.json").read_text())
    prepare(policy, args.ref, args.output, args.public)
