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

# jsonv2 is what makes the wire's `case:strict` tags do anything. Built
# without it they are parsed and ignored, so a released binary would read
# field names case-insensitively while the test suite asserted it does
# not — the strictness would exist only on the machine that ran the
# tests. Exported here so every cross-build below inherits it, and
# asserted by internal/wire's own guard test, which fails rather than
# skips when it is missing.
export GOEXPERIMENT="${GOEXPERIMENT:-jsonv2}"

KEY="${MCP_HUB_SIGNING_KEY:-$HOME/.config/mcp-hub/release-signing.key}"
PKG="github.com/secforge/mcp-hub/internal/version"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# The public half, for deriving the manifest's keyId. Defaults beside the
# private key; a release can point elsewhere. Absent means no keyId,
# which is exactly right for the original key and wrong the moment a
# second one exists — so its absence is reported rather than assumed.
PUBKEY="${MCP_HUB_SIGNING_PUBKEY:-${KEY}.pub}"
if [[ ! -f "$PUBKEY" ]]; then
	echo "note: no public key at $PUBKEY, so the manifest will carry no keyId." >&2
	echo "      Correct while this project has one signing key; set MCP_HUB_SIGNING_PUBKEY" >&2
	echo "      once there is more than one." >&2
	PUBKEY=""
fi

[[ -f "$KEY" ]] || {
	echo "refusing: no signing key at $KEY — an unsigned release cannot be installed by any client" >&2
	exit 1
}

echo "==> testing with GOEXPERIMENT=$GOEXPERIMENT"
go test ./... >/dev/null || {
	echo "refusing: the suite does not pass under GOEXPERIMENT=$GOEXPERIMENT" >&2
	exit 1
}

echo "==> fetching tags"
git fetch --tags --quiet

if [[ -n "$(git status --porcelain)" ]]; then
	echo "refusing: the tree has uncommitted changes, so the binary would embed vcs.modified" >&2
	git status --short >&2
	exit 1
fi

# LastRelease is what a DEVELOPMENT build numbers itself from
# (2.4.1.20260916170302), so it has to name the release about to be cut —
# not the one before it. Checked rather than edited here: editing it would
# dirty the tree this script just refused to build from, so the bump
# belongs in the commit being released.
CURRENT_LAST="$(go run ./internal/version/cmd/last 2>/dev/null || true)"
if [[ "$CURRENT_LAST" != "$VERSION" ]]; then
	echo "refusing: internal/version.LastRelease is ${CURRENT_LAST:-unset}, not $VERSION —" >&2
	echo "          dev builds would number themselves from the wrong release." >&2
	echo "          Update the constant, commit it, then run this again." >&2
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
	GOOS="$GOOS" GOARCH="$GOARCH" CGO_ENABLED=0 GOEXPERIMENT="$GOEXPERIMENT" \
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

# The tag is created at the COMMIT THAT WAS BUILT, named explicitly.
# Without --target, a new tag is created at the remote default branch's
# head — so a release cut from a feature branch, or from a commit that
# was never pushed, tags something other than the code inside the assets
# it publishes. The binaries would be signed, verified, and wrong.
HEAD_SHA="$(git rev-parse HEAD)"
if ! git branch -r --contains "$HEAD_SHA" >/dev/null 2>&1 || \
	[[ -z "$(git branch -r --contains "$HEAD_SHA" 2>/dev/null)" ]]; then
	echo "refusing: HEAD ($HEAD_SHA) is not on any remote branch, so the tag would point at" >&2
	echo "          something the assets were not built from. Push first." >&2
	exit 1
fi

# THE MANIFEST, built after every binary exists and before anything is
# published. It states the version and each binary's sha256, signed with
# the same key — which is what makes the version a signed fact rather
# than a tag anybody could serve beside old bytes. The per-binary .sig
# files stay published for anyone verifying a download by hand; the
# client reads this instead.
echo "==> building and signing the release manifest"
go run ./internal/selfupdate/cmd/manifest \
	-version "$VERSION" -commit "$HEAD_SHA" -dir "$OUT" -key "$KEY" \
	${PUBKEY:+-public-key "$PUBKEY"} -expect "${#PLATFORMS[@]}" \
	-out "$OUT/mcp-hub-release-manifest.json"

# Verified here, against the same public key the client has compiled in,
# for the same reason every binary is: a manifest that does not verify
# would be caught by every client that tried to update rather than by
# the release that published it.
go run ./internal/selfupdate/cmd/sign -verify \
	-in "$OUT/mcp-hub-release-manifest.json" \
	-sig "$OUT/mcp-hub-release-manifest.json.sig"

echo "==> publishing $VERSION at $HEAD_SHA"
gh release create "$VERSION" "$OUT"/* --title "$VERSION" --target "$HEAD_SHA" "$@"
echo "==> done: $(gh release view "$VERSION" --json url --jq .url)"

# THE RELEASE IS NOT REACHABLE UNTIL CHAT-RELAY RE-READS IT. That server
# serves the client binaries and caches what it last read from GitHub, so
# until it refreshes, a published release and one that was never cut look
# identical to every client that would update to it. The owner's rule,
# 2026-09-20: every release triggers the refresh.
#
# It runs LAST, after the release exists, because it asks the server to go
# and read what was just published.
#
# A failure here does NOT fail the release. The release is already
# published and signed at this point; there is nothing to roll back and
# exiting non-zero would report a successful publish as a failed one.
# What a failure costs is freshness — chat-relay keeps serving the
# previous verified value rather than a wrong one — so it is reported
# loudly and left for a human to repeat.
REFRESH_URL="${MCP_HUB_RELEASE_REFRESH_URL:-https://chat-relay.secforge.de/api/hub/client-release/refresh}"
if [[ -z "$REFRESH_URL" ]]; then
	echo "==> skipping the chat-relay refresh: MCP_HUB_RELEASE_REFRESH_URL is empty" >&2
else
	echo "==> asking chat-relay to re-read the release"
	if REFRESH_OUT="$(curl -fsS --max-time 30 -X POST "$REFRESH_URL" 2>&1)"; then
		echo "    $REFRESH_OUT"
		# Reported, not parsed. The server answering is what was asked
		# for; asserting the shape of its answer here would make this
		# script fail on a change to a field it does not own.
		echo "==> chat-relay refreshed. Check the version above names $VERSION; if it does not,"
		echo "    the refresh ran before GitHub served the new release and wants repeating."
	else
		echo "WARNING: the chat-relay refresh failed: $REFRESH_OUT" >&2
		echo "WARNING: $VERSION IS published and signed, but chat-relay is still serving whatever" >&2
		echo "         it last read. Clients will not see this release until someone repeats:" >&2
		echo "             curl -fsS -X POST $REFRESH_URL" >&2
	fi
fi
