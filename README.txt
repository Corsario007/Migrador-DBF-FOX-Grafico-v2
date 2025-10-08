# Migrador FoxPro → PostgreSQL (MACR Technologies)

Esta aplicación permite migrar tablas .DBF (Visual FoxPro 9) hacia PostgreSQL de forma segura y automática.

## Contenido
- migrador_dbf_postgres_gui.py → código fuente principal
- icono.ico → ícono corporativo MACR Technologies
- version.txt → información de versión para PyInstaller

## Compilación
Ejecuta en consola:
pyinstaller --noconsole --onefile --icon=icono.ico --version-file=version.txt --name "Migrador FoxPro" migrador_dbf_postgres_gui.py

© 2025 MACR Technologies. Todos los derechos reservados.
