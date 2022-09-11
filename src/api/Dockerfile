FROM python:3.9
ADD . /api
WORKDIR /api
EXPOSE 5000
RUN pip install -r requirements.txt
CMD python api.py