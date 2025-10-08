import os
import threading
import psycopg2
from dbfread import DBF
from tqdm import tqdm
import tkinter as tk
from tkinter import filedialog, scrolledtext, messagebox, PhotoImage
import time

# ==================================================
# FUNCIÓN DE LOG EN ARCHIVO
# ==================================================
def write_log(message):
    timestamp = time.strftime("[%Y-%m-%d %H:%M:%S] ")
    with open("migracion.log", "a", encoding="utf-8") as f:
        f.write(timestamp + message + "\n")

# ==================================================
# FUNCIÓN DE MIGRACIÓN
# ==================================================
def migrate_dbf_to_postgres(config, folder, log_widget):
    try:
        conn = psycopg2.connect(**config)
        cur = conn.cursor()
        schema = config.get("schema", "public")

        # Crear el esquema si no existe
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')
        conn.commit()

        msg = "🦊 Migrador FoxPro (.dbf) → PostgreSQL"
        log_widget.insert(tk.END, msg + "\n\n")
        write_log(msg)

        msg = f"Conectado a {config['dbname']} en {config['host']}"
        log_widget.insert(tk.END, msg + "\n\n")
        write_log(msg)

        for file in os.listdir(folder):
            if file.lower().endswith(".dbf"):
                table_name = os.path.splitext(file)[0].lower()
                full_table_name = f'"{schema}"."{table_name}"'
                dbf_path = os.path.join(folder, file)

                msg = f"➡ Migrando tabla: {table_name}"
                log_widget.insert(tk.END, msg + "\n")
                log_widget.see(tk.END)
                log_widget.update()
                write_log(msg)

                try:
                    dbf = DBF(dbf_path, encoding='latin1')

                    # Crear tabla
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

                    # Insertar registros
                    count = 0
                    for record in tqdm(dbf, desc=f"{table_name}", unit="reg"):
                        cols = ', '.join(f'"{k.lower()}"' for k in record.keys())
                        vals = ', '.join(["%s"] * len(record))
                        sql = f'INSERT INTO {full_table_name} ({cols}) VALUES ({vals})'
                        cur.execute(sql, list(record.values()))
                        count += 1

                    conn.commit()
                    msg = f"✔ {count} registros migrados en {table_name}"
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

        cur.close()
        conn.close()
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
# INTERFAZ TKINTER
# ==================================================
def start_gui():
    root = tk.Tk()
    root.title("Migrador FoxPro → PostgreSQL")
    root.geometry("960x540")
    root.resizable(False, False)

    # Campos de conexión
    frame_conn = tk.LabelFrame(root, text="Conexión PostgreSQL", padx=10, pady=10)
    frame_conn.pack(padx=10, pady=10, fill="x")

    tk.Label(frame_conn, text="Base de datos:").grid(row=0, column=0, sticky="w")
    tk.Label(frame_conn, text="Usuario:").grid(row=1, column=0, sticky="w")
    tk.Label(frame_conn, text="Contraseña:").grid(row=2, column=0, sticky="w")
    tk.Label(frame_conn, text="Host:").grid(row=0, column=2, sticky="w")
    tk.Label(frame_conn, text="Puerto:").grid(row=1, column=2, sticky="w")
    tk.Label(frame_conn, text="Esquema:").grid(row=2, column=2, sticky="w")

    db_name = tk.Entry(frame_conn, width=18)
    user = tk.Entry(frame_conn, width=18)
    password = tk.Entry(frame_conn, width=18, show="*")
    host = tk.Entry(frame_conn, width=18)
    port = tk.Entry(frame_conn, width=8)
    schema = tk.Entry(frame_conn, width=18)

    db_name.grid(row=0, column=1)
    user.grid(row=1, column=1)
    password.grid(row=2, column=1)
    host.grid(row=0, column=3)
    port.grid(row=1, column=3)
    schema.grid(row=2, column=3)

    db_name.insert(0, "postgres")
    user.insert(0, "postgres")
    password.insert(0, "")
    host.insert(0, "localhost")
    port.insert(0, "5432")
    schema.insert(0, "public")

    # Selección de carpeta
    frame_folder = tk.LabelFrame(root, text="Carpeta de archivos DBF", padx=10, pady=10)
    frame_folder.pack(padx=10, pady=5, fill="x")

    folder_path = tk.StringVar()

    def choose_folder():
        path = filedialog.askdirectory()
        if path:
            folder_path.set(path)

    tk.Entry(frame_folder, textvariable=folder_path, width=55).pack(side="left", padx=5)
    tk.Button(frame_folder, text="Seleccionar...", command=choose_folder).pack(side="left")

    # Área de log
    log_widget = scrolledtext.ScrolledText(root, width=125, height=15, state="normal")
    log_widget.pack(padx=10, pady=10)

    # Botón de migrar
    def start_migration():
        if not folder_path.get():
            messagebox.showwarning("Advertencia", "Selecciona una carpeta con archivos DBF.")
            return

        config = {
            "dbname": db_name.get(),
            "user": user.get(),
            "password": password.get(),
            "host": host.get(),
            "port": port.get(),
            "schema": schema.get()
        }

        log_widget.insert(tk.END, "🚀 Iniciando migración...\n\n")
        log_widget.see(tk.END)
        write_log("🚀 Iniciando migración...")
        threading.Thread(target=migrate_dbf_to_postgres, args=(config, folder_path.get(), log_widget), daemon=True).start()

    tk.Button(root, text="Iniciar Migración", command=start_migration, bg="#4CAF50", fg="white", padx=10, pady=5).pack(pady=10)

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