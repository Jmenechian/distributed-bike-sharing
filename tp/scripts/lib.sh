#!/usr/bin/env bash
# Helpers compartidos para los scripts de prueba de elección de líder (caja negra).
# Corren el binario de debug real contra los configs de src/bin y validan por log.
#
# Nota: usan los timeouts de PRODUCCIÓN (liveness 15s), así que son lentos (entre 30-60s).
# Como bindean los puertos fijos 6001-6004, no corras estos scripts a la vez que un
# cluster manual ni que el test de integración de Rust.

set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/target/debug/procesador_pagos"
CFG="$ROOT/src/bin/procesador_pagos"
LOGDIR="$(mktemp -d)"

declare -A PIDS
FALLOS=0

requiere_binario() {
    if [[ ! -x "$BIN" ]]; then
        echo "No existe $BIN. Compilá primero:  (cd \"$ROOT\" && cargo build)"
        exit 1
    fi
}

# levantar <archivo_config> <id>
levantar() {
    "$BIN" "$CFG/$1" > "$LOGDIR/$2.log" 2>&1 &
    PIDS[$2]=$!
    echo "  levantado nodo $2 (pid ${PIDS[$2]})"
}

# matar <id>
matar() {
    if [[ -n "${PIDS[$1]:-}" ]]; then
        kill "${PIDS[$1]}" 2>/dev/null
        unset 'PIDS[$1]'
        echo "  matado nodo $1"
    fi
}

matar_todos() {
    for id in "${!PIDS[@]}"; do
        kill "${PIDS[$id]}" 2>/dev/null
    done
}

# esperar <segundos> [comentario]
esperar() {
    echo "  esperando ${1}s ${2:-}"
    sleep "$1"
}

# assert_log <id> <patron_regex> <descripcion>
# Los nodos escriben su log con log! en $ROOT/logs/procesador<id>.log, no en el stdout
# redirigido por levantar (que solo recibe el banner de la CLI).
assert_log() {
    local archivo="$ROOT/logs/procesador$1.log"
    if grep -qE "$2" "$archivo"; then
        echo "  PASS: $3"
    else
        echo "  FAIL: $3"
        echo "        (no aparece /$2/ en el log del nodo $1: $archivo)"
        FALLOS=$((FALLOS + 1))
    fi
}

resumen() {
    echo
    if [[ "$FALLOS" -eq 0 ]]; then
        echo "RESULTADO: OK"
        rm -rf "$LOGDIR"
        exit 0
    fi
    echo "RESULTADO: $FALLOS asercion(es) fallaron. Logs en $LOGDIR"
    exit 1
}

# Matar todo lo que quede al salir (fin normal o Ctrl-C).
trap matar_todos EXIT
