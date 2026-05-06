#!/usr/bin/env sh
set -eu

repo="${CAPSULE_REPO:-limiteinductive/capsule}"
version="${CAPSULE_VERSION:-latest}"
prefix="${PREFIX:-"$HOME/.local"}"
bin_dir="${BIN_DIR:-"$prefix/bin"}"

case "$(uname -s):$(uname -m)" in
Linux:x86_64)
    asset="capsule-x86_64-unknown-linux-gnu"
    ;;
Darwin:arm64 | Darwin:aarch64)
    asset="capsule-aarch64-apple-darwin"
    ;;
*)
    echo "unsupported platform: $(uname -s) $(uname -m)" >&2
    echo "download a release asset manually from https://github.com/$repo/releases" >&2
    exit 1
    ;;
esac

base_url="https://github.com/$repo/releases"
if [ "$version" = "latest" ]; then
    download_url="$base_url/latest/download/$asset"
    checksum_url="$base_url/latest/download/$asset.sha256"
else
    download_url="$base_url/download/$version/$asset"
    checksum_url="$base_url/download/$version/$asset.sha256"
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT INT TERM

curl -fsSL "$download_url" -o "$tmp_dir/capsule"
curl -fsSL "$checksum_url" -o "$tmp_dir/capsule.sha256"

expected="$(awk '{print $1}' "$tmp_dir/capsule.sha256")"
if command -v sha256sum >/dev/null 2>&1; then
    actual="$(sha256sum "$tmp_dir/capsule" | awk '{print $1}')"
else
    actual="$(shasum -a 256 "$tmp_dir/capsule" | awk '{print $1}')"
fi

if [ "$actual" != "$expected" ]; then
    echo "checksum mismatch for $asset" >&2
    echo "expected: $expected" >&2
    echo "actual:   $actual" >&2
    exit 1
fi

mkdir -p "$bin_dir"
install -m 0755 "$tmp_dir/capsule" "$bin_dir/capsule"
echo "installed capsule to $bin_dir/capsule"
