FROM ubuntu:19.10
ENV DEBIAN_FRONTEND noninteractive
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

RUN apt-get update
RUN apt-get -y --fix-missing upgrade
RUN apt-get install -y --fix-missing build-essential gfortran cmake zip wget git
RUN apt-get install -y --fix-missing python3-dev python3-pip
RUN apt-get install -y --fix-missing libpq-dev libpng-dev
RUN apt-get install -y --fix-missing python3-opencv

# timezone stuff, again because corporate proxy
ENV TZ 'Australia/Sydney'
RUN echo $TZ > /etc/timezone && \
    apt-get install -y tzdata && \
    rm /etc/localtime && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata


# Install all the python libraries we'll need that are
# handled by pip
RUN pip3 install --proxy=http://aucloudproxy.aap.pwcinternal.com:3128 cython scipy numpy pandas
RUN pip3 install --proxy=http://aucloudproxy.aap.pwcinternal.com:3128 matplotlib
RUN pip3 install --proxy=http://aucloudproxy.aap.pwcinternal.com:3128 psycopg2

RUN mkdir /usr/src/module/
WORKDIR /usr/src/module/
COPY module .
RUN pip3 install --proxy=http://aucloudproxy.aap.pwcinternal.com:3128 -e .

RUN mkdir /usr/src/app
WORKDIR /usr/src/app/
COPY app .


# for running the apps; should just have a requirements.txt file but oh well
RUN pip3 install --proxy=http://aucloudproxy.aap.pwcinternal.com:3128 flask flask_cors waitress

