# encoding: utf-8

import logging
import json
from datetime import datetime, timedelta

import airflow
from airflow import settings, configuration
from airflow.utils.db import provide_session
from airflow.models import DagModel
from sqlalchemy import (
    Column, Integer, String, DateTime, Text, Boolean, ForeignKey, PickleType,
    Index, Float, inspect)
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.event import listen
from sqlalchemy.orm import sessionmaker, synonym


Base = declarative_base()
ID_LEN = 250


def get_current_user():
    try:
        user = airflow.login.current_user.user
    except Exception as e:
        user = None
    return user


class UserGroup(Base):
    __tablename__ = "ugmp_user_group"

    id = Column(Integer, primary_key=True)
    username = Column(String(ID_LEN), nullable=False, unique=True)
    group = Column(String(ID_LEN), nullable=False, unique=True)
    creator_user_id = Column(Integer, nullable=False)
    creator_user_name = Column(String(ID_LEN), nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    created_at = Column(DateTime, default=datetime.now)

    def __repr__(self):
        return "<UserGroup: %s>" % self.created_at
