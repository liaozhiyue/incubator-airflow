# encoding: utf-8

from datetime import datetime

import airflow
from sqlalchemy import (
    Column, Integer, String, DateTime, Text, Boolean, ForeignKey, PickleType,
    Index, Float, inspect)
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.schema import UniqueConstraint


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
    user_name = Column(String(ID_LEN), nullable=False)
    group = Column(String(ID_LEN), nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    created_at = Column(DateTime, default=datetime.now)

    UniqueConstraint('user_name', 'group', name='user_group')

    def __repr__(self):
        return "<UserGroup: %s>" % self.created_at
