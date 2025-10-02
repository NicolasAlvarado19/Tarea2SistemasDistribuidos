#!/usr/bin/env python3
import csv, time, sys, json, pathlib, argparse
from urllib import request, error
from itertools import islice

ALMACENAMIENTO_URL = "http://localhost:8000/guardar-qa"

def post_json(url, data, timeout=60):
    body = json.dumps(data).encode("utf-8")
    req = request.Request(url, data=body, headers={"Content-Type": "application/json"})
    return request.urlopen(req, timeout=timeout)

def main():
    ap = argparse.ArgumentParser(description="Carga preguntas/respuestas a /guardar-qa")
    ap.add_argument("--csv", default="datos/preguntas_10k.csv", help="Ruta del CSV")
    ap.add_argument("--inicio", type=int, default=0, help="Fila inicial (0 = primera)")
    ap.add_argument("--limite", type=int, default=None, help="Máximo de filas a enviar")
    ap.add_argument("--sleep", type=float, default=0.3, help="Pausa entre requests (s)")
    ap.add_argument("--reintentos", type=int, default=3, help="Reintentos por fila")
    args = ap.parse_args()

    ruta = pathlib.Path(args.csv)
    if not ruta.exists():
        print(f"❌ No existe {ruta}")
        sys.exit(1)

    total_previsto = sum(1 for _ in open(ruta, encoding="utf-8")) - 1  # sin header
    if args.limite is not None:
        total_previsto = min(total_previsto - args.inicio, args.limite)

    exito = fallo = 0
    with open(ruta, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # saltar hasta 'inicio'
        reader = islice(reader, args.inicio, None)
        # limitar si se pide
        if args.limite is not None:
            reader = islice(reader, args.limite)

        for i, row in enumerate(reader, start=1):
            payload = {
                "pregunta": row["pregunta"],
                "respuesta_original": row["mejor_respuesta"],
            }
            for intento in range(1, args.reintentos + 1):
                try:
                    post_json(ALMACENAMIENTO_URL, payload, timeout=120).read()
                    exito += 1
                    break
                except error.HTTPError as e:
                    fallo += 1 if intento == args.reintentos else 0
                    print(f"\nHTTP {e.code} en fila {args.inicio + i} (intento {intento}/{args.reintentos})")
                    if e.code in (429, 500, 502, 503, 504):
                        time.sleep(min(5 * intento, 20))
                except Exception as e:
                    fallo += 1 if intento == args.reintentos else 0
                    print(f"\nError en fila {args.inicio + i}: {e} (intento {intento}/{args.reintentos})")
                    time.sleep(min(5 * intento, 20))
            # progreso simple
            print(f"\rProgreso: {exito + fallo}/{total_previsto if total_previsto>0 else '?'} ok={exito} err={fallo}", end="")
            time.sleep(args.sleep)
    print("\n✅ Terminado. Éxitos:", exito, "| Fallos:", fallo)

if __name__ == "__main__": ##########
    main()