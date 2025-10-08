import os
import threading
import psycopg2
from dbfread import DBF
from tqdm import tqdm
import tkinter as tk
from tkinter import filedialog, scrolledtext, messagebox, PhotoImage, ttk
import time
import logging
import csv

# ==================================================
# CONFIGURACIÓN DE LOGGING
# ==================================================
logging.basicConfig(
    filename="migracion.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    encoding="utf-8"
)

def write_log(message):
    logging.info(message)

# ==================================================
# VARIABLE DE CANCELACIÓN
# ==================================================
cancel_migration = threading.Event()

# ==================================================
# SANITIZAR VALORES
# ==================================================
from datetime import datetime

def sanitize_value(val, field_type=None):
    if isinstance(val, bytes):
        # Detectar si son solo bytes nulos
        if val == b'\x00' * len(val):
            return None
        try:
            decoded = val.decode('latin1').strip()
            if field_type == "N":
                try:
                    return float(decoded)
                except ValueError:
                    return None
            elif field_type == "D":
                try:
                    return datetime.strptime(decoded, "%Y%m%d").date()
                except ValueError:
                    return None
            elif field_type == "L":
                return decoded.upper() in ("Y", "T")
            return decoded if decoded else None
        except Exception:
            return None

    elif isinstance(val, str):
        val = val.strip()
        if field_type == "N":
            try:
                return float(val)
            except ValueError:
                return None
        elif field_type == "D":
            try:
                return datetime.strptime(val, "%Y%m%d").date()
            except ValueError:
                return None
        elif field_type == "L":
            return val.upper() in ("Y", "T")
        return val

    elif isinstance(val, (int, float, bool)):
        return val

    elif val is None:
        return None

    else:
        try:
            val_str = str(val).strip()
            if field_type == "N":
                try:
                    return float(val_str)
                except ValueError:
                    return None
            elif field_type == "D":
                try:
                    return datetime.strptime(val_str, "%Y%m%d").date()
                except ValueError:
                    return None
            elif field_type == "L":
                return val_str.upper() in ("Y", "T")
            return val_str if val_str else None
        except Exception:
            return None

# ==================================================
# MIGRACIÓN DBF → POSTGRES
# ==================================================
def migrate_dbf_to_postgres(config, schema, folder, log_widget, progress_bar):
    try:
        conn = psycopg2.connect(**config)
        cur = conn.cursor()

        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')
        conn.commit()

        dbf_files = [f for f in os.listdir(folder) if f.lower().endswith(".dbf")]
        total_files = len(dbf_files)
        progress_bar["maximum"] = total_files

        log_widget.insert(tk.END, f"Conectado a {config['dbname']} en {config['host']} usando esquema '{schema}'\n\n")
        write_log(f"Conectado a {config['dbname']} en {config['host']} usando esquema '{schema}'")
        log_widget.insert(tk.END, "🦊 Migrador FoxPro (.dbf) → PostgreSQL\n\n")
        write_log("🦊 Migrador FoxPro (.dbf) → PostgreSQL")

        for file in dbf_files:
            if cancel_migration.is_set():
                msg = "⚠️ Migración cancelada por el usuario."
                log_widget.insert(tk.END, msg + "\n")
                log_widget.see(tk.END)
                write_log(msg)
                break

            table_name = os.path.splitext(file)[0].lower()
            full_table_name = f'"{schema}"."{table_name}"'
            dbf_path = os.path.join(folder, file)

            log_widget.insert(tk.END, f"➡ Migrando tabla: {table_name}\n")
            log_widget.see(tk.END)
            log_widget.update()
            write_log(f"Migrando tabla: {table_name}")

            try:
                dbf = DBF(dbf_path, encoding='latin1')

                fields = []
                for f in dbf.fields:
                    ftype = "TEXT"
                    if f.type == "N":
                        ftype = "NUMERIC"
                    elif f.type == "D":
                        ftype = "DATE"
                    elif f.type == "L":
                        ftype = "BOOLEAN"
                    fields.append(f'"{f.name.lower()}" {ftype}')

                create_sql = f'CREATE TABLE IF NOT EXISTS {full_table_name} ({", ".join(fields)});'
                cur.execute(create_sql)
                conn.commit()

                records_to_insert = []
                for record in tqdm(dbf, desc=f"{table_name}", unit="reg"):
                    sanitized_values = [
                        sanitize_value(record[f.name], f.type) for f in dbf.fields
                    ]
                    records_to_insert.append(sanitized_values)

                cols = ', '.join(f'"{k.lower()}"' for k in dbf.field_names)
                vals = ', '.join(["%s"] * len(dbf.field_names))
                sql = f'INSERT INTO {full_table_name} ({cols}) VALUES ({vals})'

                cur.executemany(sql, records_to_insert)
                conn.commit()

                msg = f"✔ {len(records_to_insert)} registros migrados en {table_name}"
                log_widget.insert(tk.END, msg + "\n\n")
                log_widget.see(tk.END)
                log_widget.update()
                write_log(msg)

            except Exception as e:
                msg = f"❌ Error en {table_name}: {e}"
                log_widget.insert(tk.END, msg + "\n\n")
                log_widget.see(tk.END)
                write_log(msg)
                conn.rollback()

            progress_bar["value"] += 1
            progress_bar.update()

        cur.close()
        conn.close()
        if not cancel_migration.is_set():
            msg = "✅ Migración completada exitosamente."
            log_widget.insert(tk.END, msg + "\n")
            log_widget.see(tk.END)
            write_log(msg)
            messagebox.showinfo("Completado", "Migración finalizada con éxito.")

    except Exception as e:
        msg = f"No se pudo conectar a PostgreSQL:\n{e}"
        write_log("❌ " + msg)
        messagebox.showerror("Error de conexión", msg)

# ==================================================
# INTERFAZ GRÁFICA
# ==================================================
def start_gui():
    root = tk.Tk()
    root.title("Migrador FoxPro → PostgreSQL")
    root.geometry("960x640")
    root.resizable(False, False)

    # Frame de conexión
    frame_conn = tk.LabelFrame(root, text="Conexión PostgreSQL", padx=10, pady=10)
    frame_conn.grid(row=0, column=0, padx=10, pady=10, sticky="ew")

    for i in range(4):
        frame_conn.columnconfigure(i, weight=1)

    tk.Label(frame_conn, text="Base de datos:").grid(row=0, column=0, sticky="w")
    db_name = tk.Entry(frame_conn)
    db_name.grid(row=0, column=1, sticky="ew")

    tk.Label(frame_conn, text="Usuario:").grid(row=1, column=0, sticky="w")
    user = tk.Entry(frame_conn)
    user.grid(row=1, column=1, sticky="ew")

    tk.Label(frame_conn, text="Contraseña:").grid(row=2, column=0, sticky="w")
    password = tk.Entry(frame_conn, show="*")
    password.grid(row=2, column=1, sticky="ew")

    tk.Label(frame_conn, text="Host:").grid(row=0, column=2, sticky="w")
    host = tk.Entry(frame_conn)
    host.grid(row=0, column=3, sticky="ew")

    tk.Label(frame_conn, text="Puerto:").grid(row=1, column=2, sticky="w")
    port = tk.Entry(frame_conn)
    port.grid(row=1, column=3, sticky="ew")

    tk.Label(frame_conn, text="Esquema:").grid(row=2, column=2, sticky="w")
    schema = tk.Entry(frame_conn)
    schema.grid(row=2, column=3, sticky="ew")

    db_name.insert(0, "postgres")
    user.insert(0, "postgres")
    password.insert(0, "")
    host.insert(0, "localhost")
    port.insert(0, "5432")
    schema.insert(0, "public")

    # Frame de carpeta
    frame_folder = tk.LabelFrame(root, text="Carpeta de archivos DBF", padx=10, pady=10)
    frame_folder.grid(row=1, column=0, padx=10, pady=5, sticky="ew")
    frame_folder.columnconfigure(0, weight=1)

    folder_path = tk.StringVar()

    folder_entry = tk.Entry(frame_folder, textvariable=folder_path)
    folder_entry.grid(row=0, column=0, sticky="ew", padx=5)
    tk.Button(frame_folder, text="Seleccionar...", command=lambda: folder_path.set(filedialog.askdirectory())).grid(row=0, column=1, padx=5)

    # Área de log
    log_widget = scrolledtext.ScrolledText(root, width=120, height=15)
    log_widget.grid(row=2, column=0, padx=10, pady=10, sticky="nsew")

    # Barra de progreso
    progress_frame = tk.Frame(root)
    progress_frame.grid(row=3, column=0, padx=10, pady=5, sticky="ew")
    tk.Label(progress_frame, text="Progreso:").pack(side="left", padx=5)
    progress_bar = ttk.Progressbar(progress_frame, orient="horizontal", length=800, mode="determinate")
    progress_bar.pack(side="left", padx=5, fill="x", expand=True)

    # Botones de acción
    action_frame = tk.Frame(root)
    action_frame.grid(row=4, column=0, padx=10, pady=10, sticky="ew")

    def start_migration():
        if not folder_path.get():
            messagebox.showwarning("Advertencia", "Selecciona una carpeta con archivos DBF.")
            return

        cancel_migration.clear()
        config = {
            "dbname": db_name.get(),
            "user": user.get(),
            "password": password.get(),
            "host": host.get(),
            "port": port.get()
        }
        schema_name = schema.get()

        log_widget.insert(tk.END, "🚀 Iniciando migración...\n\n")
        log_widget.see(tk.END)
        write_log("🚀 Iniciando migración...")
        threading.Thread(
            target=migrate_dbf_to_postgres,
            args=(config, schema_name, folder_path.get(), log_widget, progress_bar),
            daemon=True
        ).start()

    def clear_log():
        log_widget.delete(1.0, tk.END)

    def cancel_migration_action():
        cancel_migration.set()
        messagebox.showinfo("Cancelado", "La migración será detenida.")

    def export_log_to_csv():
        log_text = log_widget.get("1.0", tk.END).strip().split("\n")
        if not log_text:
            messagebox.showwarning("Vacío", "No hay mensajes para exportar.")
            return
        try:
            with open("log_exportado.csv", "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["Mensaje"])
                for line in log_text:
                    writer.writerow([line])
            messagebox.showinfo("Exportado", "Log exportado como 'log_exportado.csv'.")
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo exportar el log:\n{e}")

    # Botones
    tk.Button(action_frame, text="Iniciar Migración", command=start_migration, bg="#4CAF50", fg="white", padx=10, pady=5).pack(side="left", padx=5)
    tk.Button(action_frame, text="Cancelar Migración", command=cancel_migration_action, bg="#FF9800", fg="white", padx=10, pady=5).pack(side="left", padx=5)
    tk.Button(action_frame, text="Limpiar Log", command=clear_log, bg="#f44336", fg="white", padx=10, pady=5).pack(side="left", padx=5)
    tk.Button(action_frame, text="Exportar Log a CSV", command=export_log_to_csv, bg="#2196F3", fg="white", padx=10, pady=5).pack(side="left", padx=5)

    root.grid_rowconfigure(2, weight=1)
    root.grid_columnconfigure(0, weight=1)
    root.mainloop()

# ==================================================
# SPLASH SCREEN
# ==================================================
def show_splash():
    splash = tk.Tk()
    splash.overrideredirect(True)
    splash.geometry("640x480+500+250")
    splash.configure(bg="#2C3E50")

    try:
        frame = tk.Frame(splash, bg="white", padx=20, pady=20)
        frame.pack(expand=True)

        img = tk.PhotoImage(file="LogoApp.png")
        tk.Label(frame, image=img, bg="white").pack()
        tk.Label(frame, text="Migrador FoxPro → PostgreSQL", font=("Arial", 14), bg="white").pack(pady=10)
        tk.Label(frame, text="Cargando...", font=("Arial", 10), bg="white").pack()
        splash.image = img
    except:
        tk.Label(splash, text="Migrador FoxPro → PostgreSQL", font=("Arial", 16)).pack(pady=80)

    tk.Label(splash, text="Cargando aplicación...", font=("Arial", 12)).pack(pady=10)
    splash.after(2000, lambda: [splash.destroy(), start_gui()])
    splash.mainloop()

# ==================================================
# EJECUCIÓN PRINCIPAL
# ==================================================
if __name__ == "__main__":
    show_splash()