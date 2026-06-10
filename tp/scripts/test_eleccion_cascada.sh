#!/usr/bin/env bash
# Escenario: cae el líder, gana el 4; luego cae el 4 y debe re-elegirse al 3.
# Valida que el monitoreo se reconfigura tras la primera elección (gap 1).
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

requiere_binario

echo "== Escenario cascada: cae el líder, luego el nuevo líder =="
levantar lider.json 1
levantar replica.json 2
levantar replica3.json 3
levantar replica4.json 4

esperar 8 "(estabilización del cluster)"

matar 1
esperar 25 "(detección + 1ra elección)"
assert_log 4 "Gane la eleccion: me proclamo LIDER" "el nodo 4 gana la 1ra elección"

matar 4
esperar 25 "(detección del nuevo líder caído + 2da elección)"
assert_log 3 "Gane la eleccion: me proclamo LIDER" "el nodo 3 gana tras caer el 4"
assert_log 2 "Coordinador: el nuevo lider es 3" "el nodo 2 reconoce al 3 como líder"

resumen
