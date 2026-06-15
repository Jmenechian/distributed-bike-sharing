#!/usr/bin/env bash
# Pruebas end-to-end de la CLI: política de robos (PREMONTO como seguro reembolsable)
# y encolado de notificaciones con sedes online/offline.
#
# Corre los binarios reales (procesador_pagos + 2 sedes + cliente), maneja sus CLIs por
# stdin via FIFOs, y valida por stdout / logs/sede{n}.log / estado_sede_{n}.json.
#
# Uso:   scripts/test_robos_e2e.sh [e1 e2 e3 e4 e5 e6 | all]
# Tiempos: E2-E4 esperan >60s (ventana de robo), E6 ~20s extra (liveness). Total ~5 min.
#
# OJO: bindea puertos fijos (6001, 7001/2, 8081/2, 8881/2, 9091). No correr junto a un
# cluster manual ni a los otros scripts/tests.

set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/target/debug"
CFG="$ROOT/src/bin"
LOGDIR="$(mktemp -d)"
ROBO_WAIT="${ROBO_WAIT:-65}"   # > 60s (ventana de robo)

declare -A PIDS FIFO OUT
HOLDERS=()
FALLOS=0

# ---------- infra ----------

requiere_binarios() {
    for b in procesador_pagos sede cliente; do
        [[ -x "$BIN/$b" ]] || { echo "Falta $BIN/$b. Compilá: (cd \"$ROOT\" && cargo build)"; exit 1; }
    done
}

# levantar <bin> <config> <id>   (config relativo a src/bin; id = etiqueta de logs)
levantar() {
    local bin=$1 cfg=$2 id=$3
    local fifo="$LOGDIR/$id.in"
    mkfifo "$fifo"
    sleep 100000 > "$fifo" &           # holder: mantiene abierto el extremo de escritura
    HOLDERS+=($!)
    ( cd "$ROOT" && exec "$BIN/$bin" "$CFG/$cfg" ) < "$fifo" > "$LOGDIR/$id.out" 2>&1 &
    PIDS[$id]=$!
    FIFO[$id]="$fifo"
    OUT[$id]="$LOGDIR/$id.out"
    echo "  + $id ($bin $cfg) pid ${PIDS[$id]}"
}

enviar() {  # enviar <id> <comando>
    echo "  > [$1] $2" >&2   # a stderr: así no contamina las command substitutions
    echo "$2" > "${FIFO[$1]}"
    sleep 1.5
}

matar() {
    [[ -n "${PIDS[$1]:-}" ]] && kill "${PIDS[$1]}" 2>/dev/null
    unset 'PIDS[$1]'
}

matar_todos() {
    for id in "${!PIDS[@]}"; do kill "${PIDS[$id]}" 2>/dev/null; done
    for h in "${HOLDERS[@]:-}"; do kill "$h" 2>/dev/null; done
}
trap matar_todos EXIT

limpiar_estado() {
    rm -f "$ROOT"/estado_sede_*.json "$ROOT"/estado_procesador_*.json
}

esperar() { echo "  ... esperando ${1}s ${2:-}"; sleep "$1"; }

# ---------- aserciones ----------

assert_grep() {  # assert_grep <archivo> <regex> <descripcion>
    if grep -qiE "$2" "$1" 2>/dev/null; then
        echo "  PASS: $3"
    else
        echo "  FAIL: $3   (no aparece /$2/ en $1)"
        FALLOS=$((FALLOS + 1))
    fi
}

# Pide `info` a la sede y devuelve el último conteo de notificaciones pendientes que reporta.
sede_pending_count() {  # sede_pending_count <id>
    enviar "$1" "info"
    grep "Notificaciones pendientes:" "${OUT[$1]}" | tail -1 | grep -oE '[0-9]+'
}

assert_pending_no_vacio() {  # assert_pending_no_vacio <id> <descripcion>
    local n; n="$(sede_pending_count "$1")"
    if [[ "${n:-0}" -gt 0 ]]; then
        echo "  PASS: $2   (info: $n pendientes)"
    else
        echo "  FAIL: $2   (info reporta ${n:-0} pendientes)"
        FALLOS=$((FALLOS + 1))
    fi
}

assert_pending_vacio() {  # assert_pending_vacio <id> <descripcion>
    local n; n="$(sede_pending_count "$1")"
    if [[ "${n:-1}" -eq 0 ]]; then
        echo "  PASS: $2"
    else
        echo "  FAIL: $2   (info reporta ${n} pendientes)"
        FALLOS=$((FALLOS + 1))
    fi
}

assert_no_grep() {  # assert_no_grep <archivo> <regex> <descripcion>
    if grep -qiE "$2" "$1" 2>/dev/null; then
        echo "  FAIL: $3   (aparece /$2/ en $1 y no debía)"
        FALLOS=$((FALLOS + 1))
    else
        echo "  PASS: $3"
    fi
}

# ---------- setup de cluster ----------

setup_cluster() {  # setup_cluster <con_sede2: 0|1>
    limpiar_estado
    # Cluster de pagos completo: el líder corre 2PC con sus réplicas, así que necesita quórum.
    levantar procesador_pagos procesador_pagos/lider.json proc
    levantar procesador_pagos procesador_pagos/replica.json proc2
    levantar procesador_pagos procesador_pagos/replica3.json proc3
    levantar procesador_pagos procesador_pagos/replica4.json proc4
    esperar 4 "(estabilización del cluster de pagos)"
    levantar sede sede/sede1.json sede1
    [[ "${1:-1}" == "1" ]] && levantar sede sede/sede2.json sede2
    esperar 3 "(descubrimiento / heartbeats)"
}

teardown_cluster() {
    for id in cli sede1 sede2 proc proc2 proc3 proc4; do matar "$id"; done
    esperar 1
}

# ============================================================
# Escenarios
# ============================================================

e1() {
    echo "== E1: seguro reembolsable en devolución normal (online) =="
    setup_cluster 1
    levantar cliente cliente/cliente.json cli
    esperar 1
    enviar cli "conectar_sede 1"
    enviar cli "ver_dinero"
    enviar cli "solicitar_bici"
    esperar 3
    enviar cli "devolver_bici"
    enviar cli "ver_dinero"
    esperar 2
    assert_grep "${OUT[cli]}" "DevolucionOk|Devolucion exitosa|Dinero actual" "el cliente devolvió la bici"
    assert_grep "$ROOT/logs/sede1.log" "Dep.sito .* reintegrado" "sede1 reintegró el depósito (\$150)"
    assert_no_grep "${OUT[sede1]}" "Bici robada" "no se marcó robo en una devolución normal"
    teardown_cluster
}

e2() {
    echo "== E2: detección de robo + captura del depósito (online) =="
    setup_cluster 1
    levantar cliente cliente/cliente.json cli
    esperar 1
    enviar cli "conectar_sede 1"
    enviar cli "solicitar_bici"
    esperar "$ROBO_WAIT" "(ventana de robo > 60s)"
    assert_grep "${OUT[sede1]}" "Bici robada" "sede1 detectó el robo"
    assert_grep "${OUT[sede1]}" "Multa de robo retenida" "sede1 retuvo la multa (\$150)"
    assert_grep "$ROOT/estado_sede_1.json" "RoboConfirmado" "estado persistido = RoboConfirmado"
    assert_grep "$ROOT/estado_sede_1.json" "pago_multa_id\" *: *\"[0-9a-f]{8}-" "se guardó el pago_multa_id (captura aceptada)"
    teardown_cluster
}

e3() {
    echo "== E3: robo revertido cross-sede (falso positivo corregido) =="
    setup_cluster 1
    levantar cliente cliente/cliente.json cli
    esperar 1
    enviar cli "conectar_sede 1"
    enviar cli "solicitar_bici"
    esperar "$ROBO_WAIT" "(ventana de robo > 60s)"
    assert_grep "${OUT[sede1]}" "Bici robada" "sede1 marcó robo antes de la devolución tardía"
    enviar cli "conectar_sede 2"
    enviar cli "recarga 2000"   # cubrir el costo por tiempo (65s * TASA)
    enviar cli "devolver_bici"
    esperar 3
    assert_grep "${OUT[sede1]}" "Robo revertido" "sede1 revirtió el robo al recibir la devolución"
    assert_grep "$ROOT/estado_sede_1.json" "RoboRevertido" "estado persistido = RoboRevertido"
    teardown_cluster
}

e4() {
    echo "== E4: robo revertido en la misma sede (camino local) =="
    setup_cluster 1
    levantar cliente cliente/cliente.json cli
    esperar 1
    enviar cli "conectar_sede 1"
    enviar cli "solicitar_bici"
    esperar "$ROBO_WAIT" "(ventana de robo > 60s)"
    assert_grep "${OUT[sede1]}" "Bici robada" "sede1 marcó robo"
    enviar cli "recarga 2000"   # cubrir el costo por tiempo (65s * TASA)
    enviar cli "devolver_bici"
    esperar 3
    assert_grep "${OUT[sede1]}" "Robo revertido" "sede1 revirtió el robo localmente"
    assert_grep "$ROOT/estado_sede_1.json" "RoboRevertido" "estado persistido = RoboRevertido"
    assert_grep "$ROOT/estado_sede_1.json" "reembolso_multa_id\" *: *\"[0-9a-f]{8}-" "se guardó reembolso_multa_id"
    teardown_cluster
}

e5() {
    echo "== E5: sede destino OFFLINE encola y entrega al reconectar =="
    setup_cluster 1
    levantar cliente cliente/cliente.json cli
    esperar 1
    enviar cli "conectar_sede 1"
    enviar cli "solicitar_bici"
    esperar 2
    enviar sede2 "offline"
    esperar 1
    enviar cli "conectar_sede 2"
    enviar cli "devolver_bici"
    esperar 2
    assert_grep "${OUT[sede2]}" "PENDIENTE" "sede2 (offline) dejó la notificación PENDIENTE"
    assert_pending_no_vacio sede2 "sede2 reporta la notificación pendiente (via info)"
    assert_grep "${OUT[cli]}" "Dinero actual|Devolucion" "el cliente igual recibió la devolución"
    enviar sede2 "online"
    esperar 3
    assert_grep "${OUT[sede2]}" "tengo su bici" "sede2 hizo flush al volver online (avisó a sede1)"
    assert_pending_vacio sede2 "la cola de pendientes quedó vacía tras el flush (via info)"
    teardown_cluster
}

e6() {
    echo "== E6 (borde): origen caído, encola y entrega cuando revive (flush por heartbeat) =="
    setup_cluster 1
    levantar cliente cliente/cliente.json cli
    esperar 1
    enviar cli "conectar_sede 1"
    enviar cli "solicitar_bici"
    esperar 2
    matar sede1
    esperar 20 "(que sede2 marque a sede1 CAIDA, liveness 15s)"
    enviar cli "conectar_sede 2"
    enviar cli "devolver_bici"
    esperar 2
    assert_grep "${OUT[sede2]}" "PENDIENTE" "sede2 encoló (origen caído)"
    levantar sede sede/sede1.json sede1
    esperar 10 "(heartbeats: sede2 detecta a sede1 ONLINE de nuevo)"
    assert_grep "${OUT[sede2]}" "tengo su bici" "sede2 hizo flush al revivir sede1"
    teardown_cluster
}

# ============================================================

requiere_binarios
ESCENARIOS=("${@:-all}")
[[ "${ESCENARIOS[*]}" == "all" ]] && ESCENARIOS=(e1 e2 e3 e4 e5 e6)

for esc in "${ESCENARIOS[@]}"; do
    echo
    "$esc"
done

echo
if [[ "$FALLOS" -eq 0 ]]; then
    echo "RESULTADO: OK (todas las aserciones pasaron)"
    echo "Logs de la corrida en: $LOGDIR"
    exit 0
fi
echo "RESULTADO: $FALLOS aserción(es) fallaron. Logs en: $LOGDIR"
exit 1
