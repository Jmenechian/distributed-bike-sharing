#!/usr/bin/env bash
# Escenario: con 4 nodos arriba, cae el líder y debe ganar la elección el de mayor id.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

requiere_binario

echo "== Escenario simple: cae el líder, gana el de mayor id =="
levantar lider.json 1
levantar replica.json 2
levantar replica3.json 3
levantar replica4.json 4

esperar 8 "(estabilización del cluster)"
matar 1
esperar 25 "(detección de la caída + elección)"

assert_log 4 "Gane la eleccion: me proclamo LIDER" "el nodo 4 (mayor id) se proclama líder"
assert_log 2 "Coordinador: el nuevo lider es 4" "el nodo 2 reconoce al 4 como líder"
assert_log 3 "Coordinador: el nuevo lider es 4" "el nodo 3 reconoce al 4 como líder"

resumen
