import os
import pandas as pd

RUTA_ENTRADA = "test.csv"                 # CSV original (sin encabezados)
SALIDA_DIR = "datos"
SALIDA_CSV = os.path.join(SALIDA_DIR, "preguntas_10k.csv")

# Parámetros del muestreo / filtros
MIN_LEN = 20            # mínimo de caracteres de la pregunta (título+contenido)
MAX_LEN = 2000          # máximo de caracteres de la pregunta
MAX_POR_CLASE = 1000    # máximo ejemplos por clase (1..10)
SEED = 42               # para reproducibilidad


def dataset():
    print("Cargando dataset de Yahoo! Answers...")
    if not os.path.exists(RUTA_ENTRADA):
        raise SystemExit(f"No se encontró '{RUTA_ENTRADA}'. Copia aquí tu test.csv de Kaggle.")

    # Kaggle: sin encabezados → definimos nombres
    df = pd.read_csv(
        RUTA_ENTRADA,
        names=["clase", "titulo", "contenido", "mejor_respuesta"],
        encoding="utf-8"
    )
    print(f"Dataset cargado: {len(df)} filas")

    # Limpieza básica
    antes = len(df)
    df = df.dropna(subset=["clase", "titulo", "contenido", "mejor_respuesta"])
    print(f"🧹 Eliminados nulos: {antes - len(df)} (quedan {len(df)})")

    # Pregunta = título + contenido
    df["pregunta"] = df["titulo"].astype(str) + " " + df["contenido"].astype(str)

    # Filtro por longitud
    antes = len(df)
    df = df[(df["pregunta"].str.len() >= MIN_LEN) & (df["pregunta"].str.len() <= MAX_LEN)]
    print(f" Filtrado por longitud: {antes - len(df)} removidos (quedan {len(df)})")

    # Balanceo por clase (1..10)
    print("Muestreando por clase...")
    partes = []
    for clase in range(1, 11):
        grupo = df[df["clase"] == clase]
        if len(grupo) == 0:
            print(f"   • Clase {clase}: 0 disponibles (saltando)")
            continue
        n = min(len(grupo), MAX_POR_CLASE)
        partes.append(grupo.sample(n=n, random_state=SEED))
        print(f"   • Clase {clase}: {n} seleccionadas de {len(grupo)}")

    if not partes:
        raise SystemExit("No se pudo muestrear ninguna clase. Revisa el archivo de entrada.")

    df_final = pd.concat(partes).reset_index(drop=True)

    # Selección y renombre de columnas a las esperadas por tu pipeline
    df_final = df_final[["pregunta", "mejor_respuesta"]].rename(
        columns={"mejor_respuesta": "respuesta"}
    )

    # Guardado
    os.makedirs(SALIDA_DIR, exist_ok=True)
    df_final.to_csv(SALIDA_CSV, index=False, encoding="utf-8")

    print("\n Dataset preparado")
    print(f" Archivo: {SALIDA_CSV}")
    print(f" Filas: {len(df_final)}")

    # Muestra rápida
    print("\n" + "=" * 80)
    print(" EJEMPLOS")
    print("=" * 80)
    for i in range(min(3, len(df_final))):
        print("\n" + "─" * 80)
        print(f"EJEMPLO {i + 1}")
        print(" PREGUNTA:")
        print("  " + df_final.iloc[i]["pregunta"][:200] + ("..." if len(df_final.iloc[i]["pregunta"]) > 200 else ""))
        print("\n RESPUESTA:")
        print("  " + df_final.iloc[i]["respuesta"][:200] + ("..." if len(df_final.iloc[i]["respuesta"]) > 200 else ""))

    print("\n" + "=" * 80)
    print("¡Proceso completado!")
    print("=" * 80)


if __name__ == "__main__":
    dataset()