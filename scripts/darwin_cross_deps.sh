#!/bin/bash
# Prepares what a macOS (Darwin) FDW cross build on Linux needs, for use in the
# goreleaser-cross image: the Postgres server headers, a postgres executable
# to link against (-bundle_loader, as PGXS does on macOS) and a pg_config for
# the target. Homebrew's macOS bottles are the source, pinned by digest.
#
# Usage: darwin_cross_deps.sh <pg major: 14|15> <arm64|amd64> <out dir>
#
# Writes <out>/bin/pg_config and <out>/bin/postgres, plus <out>/intl (holding
# libintl.h when the Postgres build has NLS enabled, otherwise empty). Put
# <out>/bin first on PATH for the build. Downloads are cached in <out>/cache.
# amd64 also needs CC set to the cross compiler (o64-clang).
set -euo pipefail

if [ $# -ne 3 ]; then
  echo "Usage: $0 <pg major: 14|15> <arm64|amd64> <out dir>" >&2
  exit 1
fi
major=$1
arch=$2
mkdir -p "$3"
out=$(cd "$3" && pwd)

# Homebrew arm64_sequoia bottles on ghcr.io, by sha256 digest. Homebrew no
# longer builds Intel bottles, so amd64 builds use these too (see below).
case "$major" in
  14) bottle=postgresql/14 digest=712ac08614d28e980d7869afc39475958c9f434143212520b77594b8c51aff7b ;; # 14.25
  15) bottle=postgresql/15 digest=58fee85fb50d9c4c6eb997619977ab63a73a61c4d090e6f3e088cb65756dfae8 ;; # 15.20
  *) echo "Unsupported Postgres major version: $major" >&2; exit 1 ;;
esac
# libintl.h, which c.h includes when Postgres has NLS enabled (15 does).
gettext_digest=98a116d35792ccd730360ffc8c0d8fc5799b2282b96bb7696192c573f064a331 # 1.0

case "$arch" in
  arm64|amd64) ;;
  *) echo "Unsupported architecture: $arch" >&2; exit 1 ;;
esac

# fetch <ghcr.io/homebrew/core repo> <sha256> <file>
fetch() {
  if [ -f "$3" ] && echo "$2  $3" | sha256sum -c --status - 2>/dev/null; then
    return
  fi
  curl -fsSL -H "Authorization: Bearer QQ==" -o "$3.tmp" \
    "https://ghcr.io/v2/homebrew/core/$1/blobs/sha256:$2"
  echo "$2  $3.tmp" | sha256sum -c --quiet -
  mv "$3.tmp" "$3"
}

rm -rf "${out:?}/pg" "${out:?}/bin" "${out:?}/intl"
mkdir -p "$out/cache" "$out/pg" "$out/bin" "$out/intl"

fetch "$bottle" "$digest" "$out/cache/postgresql@$major.tar.gz"
tar -xzf "$out/cache/postgresql@$major.tar.gz" -C "$out/pg"
keg=$(echo "$out/pg/postgresql@$major"/*)

server=$(dirname "$(find "$keg/include" -path '*/server/pg_config.h')")
pkginclude=$(dirname "$server")
include=$(dirname "$(find "$keg/include" -name libpq-fe.h | head -1)")
global=$(find "$keg/lib" -path '*/pgxs/src/Makefile.global')
version=$(sed -n 's/^#define PG_VERSION "\(.*\)"$/\1/p' "$server/pg_config.h")
# PGXS compiles with the CFLAGS Postgres was built with, and links with
# LDFLAGS; of those, only the -Wl options apply here (-isysroot and the -L
# dirs are Homebrew paths on the build Mac).
cflags=$(sed -n 's/^CFLAGS = //p' "$global")
ldflags=$(sed -n 's/^LDFLAGS = //p' "$global" | tr ' ' '\n' | { grep '^-Wl,' || true; } | tr '\n' ' ' | sed 's/ $//')
cppflags=""

if grep -q '^#define ENABLE_NLS 1' "$server/pg_config.h"; then
  fetch gettext "$gettext_digest" "$out/cache/gettext.tar.gz"
  tar -xzf "$out/cache/gettext.tar.gz" -C "$out/cache" --wildcards '*/include/libintl.h'
  cp "$out"/cache/gettext/*/include/libintl.h "$out/intl/"
  cppflags="-I$out/intl"
fi

if [ "$arch" = arm64 ]; then
  ln -s "$keg/bin/postgres" "$out/bin/postgres"
else
  # Darwin x86_64's pg_config.h differs from arm64's only in CPU-feature
  # macros (and the version string), which the FDW doesn't use. This was
  # measured for 14.25 and 15.20 by running configure on both arches with the
  # same flags; re-check it when bumping a digest above. Each change must
  # apply, or the build stops.
  h=$server/pg_config.h
  sed -i \
    -e 's|^/\* #undef HAVE_X86_64_POPCNTQ \*/$|#define HAVE_X86_64_POPCNTQ 1|' \
    -e 's|^/\* #undef HAVE__GET_CPUID \*/$|#define HAVE__GET_CPUID 1|' \
    -e 's|^#define USE_ARMV8_CRC32C 1$|/* #undef USE_ARMV8_CRC32C */|' \
    -e 's|^/\* #undef USE_SSE42_CRC32C_WITH_RUNTIME_CHECK \*/$|#define USE_SSE42_CRC32C_WITH_RUNTIME_CHECK 1|' \
    -e 's| on aarch64-apple-darwin| on x86_64-apple-darwin|' \
    "$h"
  for m in '^#define HAVE_X86_64_POPCNTQ 1$' '^#define HAVE__GET_CPUID 1$' '^/\* #undef USE_ARMV8_CRC32C \*/$' \
           '^#define USE_SSE42_CRC32C_WITH_RUNTIME_CHECK 1$' ' on x86_64-apple-darwin'; do
    if ! grep -q "$m" "$h"; then
      echo "x86_64 pg_config.h change did not apply: $m" >&2
      exit 1
    fi
  done

  # There's no x86_64 postgres to link against, so build a stand-in
  # executable that exports the same symbol names as the arm64 one. Postgres
  # symbols then bind to the main executable at load time, as they do in a
  # native build. The only arch-specific exports are the CRC32C
  # implementations, so swap the arm64 one for the x86_64 ones; a reference
  # to an arm64-only symbol then fails this link rather than loading on Intel.
  : "${CC:?Set CC to the x86_64 Darwin cross compiler (o64-clang)}"
  mkdir -p "$out/stub"
  {
    llvm-nm -gUj "$keg/bin/postgres" | { grep -v '^_pg_comp_crc32c_armv8$' || true; }
    printf '%s\n' _pg_comp_crc32c _pg_comp_crc32c_sb8 _pg_comp_crc32c_sse42
  } | sort -u >"$out/stub/symbols.txt"
  {
    echo '.text'
    echo '.globl _pg_stub_start'
    echo '_pg_stub_start: ret'
    sed 's/.*/.globl "&"\n"&": ret/' "$out/stub/symbols.txt"
  } >"$out/stub/postgres.s"
  "$CC" -o "$out/bin/postgres" "$out/stub/postgres.s" -Wl,-e,_pg_stub_start
fi

# pg_config for the target. Only the options the FDW build uses.
cat >"$out/bin/pg_config" <<EOF
#!/bin/sh
# pg_config for Darwin $arch, written by darwin_cross_deps.sh.
for opt in "\$@"; do
  case "\$opt" in
    --version) echo 'PostgreSQL $version' ;;
    --bindir) echo '$out/bin' ;;
    --includedir) echo '$include' ;;
    --pkgincludedir) echo '$pkginclude' ;;
    --includedir-server) echo '$server' ;;
    --cflags) echo '$cflags' ;;
    --cppflags) echo '$cppflags' ;;
    --ldflags) echo '$ldflags' ;;
    *) echo "pg_config (Darwin cross build): unsupported option \$opt" >&2; exit 1 ;;
  esac
done
EOF
chmod +x "$out/bin/pg_config"

echo "Darwin $arch deps for PostgreSQL $version in $out"
