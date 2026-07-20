import os
import logging
from datetime import datetime

def _log_config():
    try:
        _BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        _BASE_DIR = os.getcwd()
    
    LOG_DIR = os.path.join(_BASE_DIR, "logs")
    os.makedirs(LOG_DIR, exist_ok=True)
    
    # Arquivo de log com data atual (dd_mm_yyyy)
    LOG_FILENAME = f"log_{datetime.now().strftime('%d_%m_%Y')}.log"
    LOG_FILEPATH = os.path.join(LOG_DIR, LOG_FILENAME)

    return LOG_FILEPATH

def show_log(message):
    """Exibe no console e grava diretamente no arquivo de log."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"{timestamp} [INFO] {message}"
    print(log_line)
    LOG_FILEPATH=_log_config()
    with open(LOG_FILEPATH, "a", encoding="utf-8") as f:
        f.write(log_line + "\n")

def main():
  LOG_FILEPATH=_log_config()


if __name__ == "__main__":
    main()
