# Базовый образ Python
FROM python:3.12-slim

# Рабочая директория
WORKDIR /app

# Установка всех зависимостей в одном слое
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    && wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor > /etc/apt/trusted.gpg.d/postgresql.gpg \
    && echo "deb http://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && echo "deb http://ftp.de.debian.org/debian sid main" >> /etc/apt/sources.list \
    && apt-get update \
    && apt-get install -y \
        postgresql-client-17 \
        mysql-client \
    && rm -rf /var/lib/apt/lists/*

# Копирование requirements.txt и установка Python-зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование кода приложения
COPY . .

# Создание директории dumps
RUN mkdir -p /app/dumps

# Команда для запуска приложения
CMD ["python", "main.py"]