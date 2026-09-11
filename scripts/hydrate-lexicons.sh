#!/usr/bin/env bash
# Reconstruct the ignored lexicon cache from lexgen.lock without changing pins
# or generated source.
set -euo pipefail

readonly script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly repo_root="$(cd -- "${script_dir}/.." && pwd)"
cd "${repo_root}"

readonly cache_dir="lexicons"
readonly lock_file="lexgen.lock"
readonly manifest_file="lexicons.manifest"
readonly work_dir="$(mktemp -d "${PWD}/.hydrate-lexicons.XXXXXX")"
readonly staged="${work_dir}/lexicons"
readonly previous="${work_dir}/previous"
readonly discarded="${work_dir}/discarded"
published=0
replacement_started=0

cleanup() {
    local status=$?
    trap - EXIT HUP INT TERM
    if (( ! published && replacement_started )); then
        if [[ -e "${cache_dir}" ]] && ! mv "${cache_dir}" "${discarded}"; then
            status=1
        fi
        if [[ -e "${previous}" ]] && ! mv "${previous}" "${cache_dir}"; then
            status=1
        fi
    fi
    rm -rf -- "${work_dir}"
    exit "${status}"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

verify_manifest() {
    local root="$1"
    local actual="${work_dir}/actual-manifest"
    (cd "${root}" && find . -type f -print0 | sort -z | xargs -0 sha256sum) > "${actual}"
    cmp -s "${actual}" "${repo_root}/${manifest_file}"
}

if [[ -d "${cache_dir}" ]] && verify_manifest "${cache_dir}"; then
    printf 'lexicon cache already matches %s\n' "${manifest_file}"
    exit 0
fi

mkdir -p "${staged}"
while read -r name source commit extra; do
    [[ -n "${name}" ]] || continue
    [[ "${name}" == \#* ]] && continue
    if [[ -z "${source}" || -z "${commit}" || -n "${extra:-}" ]]; then
        printf 'invalid lock entry for %s\n' "${name}" >&2
        exit 1
    fi
    checkout="${work_dir}/${name}"
    git init -q "${checkout}"
    git -C "${checkout}" remote add origin "${source}"
    git -C "${checkout}" fetch -q --depth 1 origin "${commit}"
    git -C "${checkout}" checkout -q --detach FETCH_HEAD
done < "${lock_file}"

copy_tree() {
    local source="$1"
    local target="$2"
    local path relative destination
    [[ -d "${source}" ]] || {
        printf 'expected lexicon directory %s to exist\n' "${source}" >&2
        return 1
    }
    while IFS= read -r -d '' path; do
        relative="${path#"${source}"/}"
        destination="${target}/${relative}"
        if [[ -e "${destination}" ]]; then
            printf 'lexicon source collision at %s\n' "${destination}" >&2
            return 1
        fi
        mkdir -p "$(dirname "${destination}")"
        cp -p "${path}" "${destination}"
    done < <(find "${source}" -type f -print0 | sort -z)
}

for source in "${work_dir}/atproto/lexicons"/*; do
    [[ -d "${source}" ]] || continue
    case "$(basename "${source}")" in
        app|chat) continue ;;
    esac
    copy_tree "${source}" "${staged}/$(basename "${source}")"
done
copy_tree "${work_dir}/bsky/lexicons" "${staged}"
copy_tree "${work_dir}/statusphere-example-app/lexicons" "${staged}"
verify_manifest "${staged}"

if [[ -e "${cache_dir}" ]]; then
    mv "${cache_dir}" "${previous}"
fi
replacement_started=1
mv "${staged}" "${cache_dir}"
published=1
