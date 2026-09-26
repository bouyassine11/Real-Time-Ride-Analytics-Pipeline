FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    streamlit==1.35.0 \
    plotly==5.22.0 \
    pandas==2.2.2 \
    psycopg2-binary==2.9.9

COPY dashboard/app.py ./dashboard/app.py

ENV PYTHONPATH=/app

EXPOSE 8501

CMD ["streamlit", "run", "dashboard/app.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--server.headless=true", \
     "--browser.gatherUsageStats=false"]
