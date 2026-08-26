FROM apache/airflow:2.10.5-python3.11

USER root

# Install Chrome and ChromeDriver dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    gnupg \
    ca-certificates \
    fonts-liberation \
    libappindicator3-1 \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libgbm-dev \
    libnss3 \
    lsb-release \
    xdg-utils \
    libxshmfence1 \
    libglu1-mesa \
    libxtst6 \
    libxss1 \
    libxrandr2 \
    libgconf-2-4 \
    libpango1.0-0 \
    libx11-xcb1 \
    && apt-get clean

# Switch to airflow user
USER airflow

# Copy requirements.txt to the container
COPY requirements.txt /opt/airflow/requirements.txt

RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
RUN pip install playwright==1.60.0
RUN playwright install
RUN pip install camoufox && python -m camoufox fetch
RUN pip install openpyxl