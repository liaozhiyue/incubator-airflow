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


class DagAvailable(DagModel):
    pass


class DagModelAvailableRecord(Base):
    __tablename__ = "dap_dag_model_available_record"

    id = Column(Integer, primary_key=True)
    _conf = Column('conf', Text, default="[]", nullable=False)
    creator_user_id = Column(Integer, nullable=False)
    creator_user_name = Column(String(ID_LEN), nullable=False)
    updated_at = Column(DateTime, index=True, default=datetime.now, onupdate=datetime.now)
    created_at = Column(DateTime, index=True, default=datetime.now)

    def __repr__(self):
        return "<DagModelAvailableRecord: %s>" % self.created_at
    
    def get_conf(self):
        try:
            res = json.loads(self._conf)
        except Exception as e:
            res = {}
        return res

    def set_conf(self, value):
        if value:
            self._conf = json.dumps(value)

    @declared_attr
    def conf(cls):
        return synonym('_conf',
                       descriptor=property(cls.get_conf, cls.set_conf))
    
    @provide_session
    def restore(self, session=None):
        for history in self.conf:
            dag_model = session.query(DagModel).filter(DagModel.dag_id==history["dag_id"]).first()
            if dag_model:
                dag_model.is_paused = history["is_paused"]
                dag_model.is_active = history["is_active"]


def update_conf(mapper, connection, target):
    need_record = False
    for attr in inspect(target).attrs:
        if attr.history.has_changes():
            if attr.key in ["is_paused", "is_active"]:
                need_record = True
                break
    if need_record:
        session = sessionmaker(autocommit=False, autoflush=False, bind=settings.engine)()
        conf = []
        for dag_model in session.query(DagModel).filter(DagModel.is_subdag == False):
            if dag_model.dag_id == target.dag_id:
                dag_model = target
            conf.append({
                "dag_id": dag_model.dag_id,
                "is_paused": dag_model.is_paused,
                "is_active": dag_model.is_active,
            })
        user = get_current_user()
        dag_model_available_record = session.query(DagModelAvailableRecord).order_by(DagModelAvailableRecord.created_at.desc()).first()
        if not dag_model_available_record or \
            (user and dag_model_available_record.creator_user_id != user.id) or \
            datetime.now() - dag_model_available_record.updated_at > timedelta(seconds=5):
            dag_model_available_record = DagModelAvailableRecord()
        dag_model_available_record.conf = conf
        if user:
            dag_model_available_record.creator_user_id = user.id
            dag_model_available_record.creator_user_name = user.username
        session.add(dag_model_available_record)
        session.commit()
        session.close()


if __package__:
    listen(DagModel, 'after_update', update_conf)
    listen(DagAvailable, 'after_update', update_conf)