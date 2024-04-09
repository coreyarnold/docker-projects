FROM python:3.9 

ADD save_topic_data.py .

RUN pip install --force-reinstall -v "paho-mqtt==1.6.1"

CMD ["python", "save_topic_data.py"]
