#!/usr/bin/env bash
# Cut a client release: build, sign, verify, publish.
#
# The checks here are the reason this is a script and not a sequence of
# commands typed by hand. Every one of them guards a way a release can end
# up claiming something untrue about itself:
#
#   - tags are fetched first, because releases cut through the GitHub API
#     tag the remote, and a local tree several releases behind will happily
#     build a binary that reports the wrong version;
#   - the tree must be clean, or the binary embeds vcs.modified and is not
#     reproducible from any commit;
#   - the built binary is ASKED its version and the answer must equal the
#     tag being published, so a typo in the ldflags fails the release
#     instead of shipping;
#   - the signature is verified here, against the same public key the
#     client has compiled in, so a key mismatch is caught before anyone
#     downloads it rather than by every client that tries to update.
set -euo pipefail

VERSION="${1:-}"
if [[ -z "$VERSION" ]]; then
	echo "usage: $0 vX.Y.Z [--notes-file FILE]" >&2
	exit 2
fi
shift
if [[ ! "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
	echo "refusing: '$VERSION' is not vX.Y.Z — the client compares releases numerically" >&2
	exit 1
fi

KEY="${MCP_HUB_SIGNING_KEY:-$HOME/.config/mcp-hub/release-signing.key}"
PKG="github.com/secforge/mcp-hub/internal/version"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

[[ -f "$KEY" ]] || {
	echo "refusing: no signing key at $KEY — an unsigned release cannot be installed by any client" >&2
	exit 1
}

echo "==> fetching tags"
git fetch --tags --quiet

if [[ -n "$(git status --porcelain)" ]]; then
	echo "refusing: the tree has uncommitted changes, so the binary would embed vcs.modified" >&2
	git status --short >&2
	exit 1
fi

if git rev-parse -q --verify "refs/tags/$VERSION" >/dev/null; then
	echo "refusing: tag $VERSION already exists" >&2
	exit 1
fi

OUT="$(mktemp -d)"
trap 'rm -rf "$OUT"' EXIT

# Every platform Go can target for this program without cgo. A user on a
# platform with no asset gets a clear "publishes no asset for this
# platform" from the updater — which is honest, and also useless to them,
# so the list is only short where the toolchain makes it short.
PLATFORMS=(
	linux/amd64
	linux/arm64
	darwin/amd64
	darwin/arm64
	windows/amd64
	windows/arm64
)

for PLATFORM in "${PLATFORMS[@]}"; do
	GOOS="${PLATFORM%/*}"
	GOARCH="${PLATFORM#*/}"
	ASSET="mcp-hub-client-$GOOS-$GOARCH"
	[[ "$GOOS" == "windows" ]] && ASSET="$ASSET.exe"
	echo "==> building $ASSET"
	GOOS="$GOOS" GOARCH="$GOARCH" CGO_ENABLED=0 \
		go build -ldflags "-X $PKG.Release=$VERSION" -o "$OUT/$ASSET" ./cmd/mcp-hub-client

	# Only meaningful for a binary that runs here; a cross-built one is
	# checked by the signature alone.
	if [[ "$GOOS/$GOARCH" == "$(go env GOOS)/$(go env GOARCH)" ]]; then
		echo "==> asking $ASSET what it is"
		REPORTED="$("$OUT/$ASSET" --version | head -1 | awk '{print $2}')"
		if [[ "$REPORTED" != "$VERSION" ]]; then
			echo "refusing: built binary reports '$REPORTED', not '$VERSION'" >&2
			exit 1
		fi
		"$OUT/$ASSET" --version | grep -q 'NOTE:' && {
			echo "refusing: $ASSET reports a caveat about its own build:" >&2
			"$OUT/$ASSET" --version >&2
			exit 1
		}
	fi

	echo "==> signing $ASSET"
	go run ./internal/selfupdate/cmd/sign -key "$KEY" -in "$OUT/$ASSET" -out "$OUT/$ASSET.sig"
	go run ./internal/selfupdate/cmd/sign -verify -in "$OUT/$ASSET" -sig "$OUT/$ASSET.sig"
done

echo "==> publishing $VERSION"
gh release create "$VERSION" "$OUT"/* --title "$VERSION" "$@"
echo "==> done: $(gh release view "$VERSION" --json url --jq .url)"
