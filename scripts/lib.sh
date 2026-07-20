#!/usr/bin/env bash
# Shared helpers for the verification scripts: colored PASS/FAIL accounting and
# a summary table. Sourced by verify-*.sh. Every script exits non-zero if any
# check failed.

set -uo pipefail

if [ -t 1 ]; then
  C_GREEN='\033[0;32m'; C_RED='\033[0;31m'; C_DIM='\033[2m'; C_BOLD='\033[1m'; C_RESET='\033[0m'
else
  C_GREEN=''; C_RED=''; C_DIM=''; C_BOLD=''; C_RESET=''
fi

_PASS=0
_FAIL=0
declare -a _RESULTS=()

# check <name> <command...> — runs the command; PASS if it exits 0.
check() {
  local name="$1"; shift
  if "$@" >/dev/null 2>&1; then
    _PASS=$((_PASS + 1))
    _RESULTS+=("PASS|$name")
    printf "  ${C_GREEN}✓${C_RESET} %s\n" "$name"
  else
    _FAIL=$((_FAIL + 1))
    _RESULTS+=("FAIL|$name")
    printf "  ${C_RED}✗${C_RESET} %s\n" "$name"
  fi
}

# assert <name> <condition-string> — PASS if the string is non-empty and "true"/nonzero.
# Usage: assert "3 pipelines" "$([ "$n" -eq 3 ] && echo ok)"
assert() {
  local name="$1" cond="$2"
  if [ -n "$cond" ]; then
    _PASS=$((_PASS + 1)); _RESULTS+=("PASS|$name"); printf "  ${C_GREEN}✓${C_RESET} %s\n" "$name"
  else
    _FAIL=$((_FAIL + 1)); _RESULTS+=("FAIL|$name"); printf "  ${C_RED}✗${C_RESET} %s\n" "$name"
  fi
}

section() { printf "\n${C_BOLD}%s${C_RESET}\n" "$1"; }

summary() {
  local total=$((_PASS + _FAIL))
  printf "\n${C_BOLD}── Summary ──${C_RESET}\n"
  for r in "${_RESULTS[@]}"; do
    local status="${r%%|*}" name="${r#*|}"
    if [ "$status" = "PASS" ]; then
      printf "  ${C_GREEN}PASS${C_RESET}  %s\n" "$name"
    else
      printf "  ${C_RED}FAIL${C_RESET}  %s\n" "$name"
    fi
  done
  printf "\n  %d/%d passed" "$_PASS" "$total"
  if [ "$_FAIL" -gt 0 ]; then
    printf "  ${C_RED}(%d FAILED)${C_RESET}\n" "$_FAIL"
    return 1
  fi
  printf "  ${C_GREEN}(all green)${C_RESET}\n"
  return 0
}

# jqget <json> <filter> — extract with jq, empty on error.
jqget() { echo "$1" | jq -r "$2" 2>/dev/null; }
