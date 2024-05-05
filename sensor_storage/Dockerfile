FROM python:3.9 

ADD save_topic_data.py .

RUN pip install --force-reinstall -v "paho-mqtt==1.6.1"
RUN pip install --force-reinstall -v "mysql-connector-python==8.0.33"
RUN pip install --force-reinstall -v "python-dotenv==1.0.0"


CMD ["python","-u","save_topic_data.py"]
