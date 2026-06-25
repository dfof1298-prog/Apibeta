# ============================================================
#  SONIK API - Dockerfile
#  يستخدم Python 3.11 (متوافق مع FastAPI)
# ============================================================

# 1. استخدام Python 3.11 (أكثر استقراراً)
FROM python:3.11-slim

# 2. تعيين مجلد العمل
WORKDIR /app

# 3. تثبيت التبعيات الأساسية
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 4. نسخ ملف المتطلبات أولاً (للاستفادة من caching)
COPY requirements.txt .

# 5. تثبيت المتطلبات
RUN pip install --no-cache-dir -r requirements.txt

# 6. نسخ باقي الملفات
COPY . .

# 7. فتح البورت
EXPOSE 8002

# 8. تشغيل التطبيق
CMD ["uvicorn", "checker_api2:app", "--host", "0.0.0.0", "--port", "8002"]
