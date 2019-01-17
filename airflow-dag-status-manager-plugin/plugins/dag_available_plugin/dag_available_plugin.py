# encoding: utf-8


from airflow import settings, configuration
from airflow.plugins_manager import AirflowPlugin
from airflow.www import utils as wwwutils
from airflow.www.app import csrf
from airflow.utils.db import provide_session
from airflow.models import DagModel
from flask import Blueprint, request, jsonify
from flask_login import flash
from flask_admin import BaseView, expose
from flask_admin.contrib.sqla import ModelView
from flask_admin.contrib.sqla.view import func
from flask_admin.actions import action

from dag_available_plugin.models import DagAvailable, DagModelAvailableRecord


class AirflowModelView(ModelView):
    list_template = 'dag_available_plugin/model_list.html'
    edit_template = 'airflow/model_edit.html'
    create_template = 'airflow/model_create.html'
    column_display_actions = True
    page_size = 500


class DagAvailableView(wwwutils.SuperUserMixin, AirflowModelView):
    verbose_name = "DAG Model"
    verbose_name_plural = "DAG Models"
    column_default_sort = 'dag_id'
    can_create = False
    can_delete = False
    can_edit = False
    column_display_actions = False
    column_list = ('dag_id', 'is_active', 'is_paused', 'owners', 'last_scheduler_run', )
    column_filters = ('dag_id', 'is_active', 'is_paused', 'last_scheduler_run', )
    form_columns = ('is_active', 'is_paused', )

    def get_query(self):
        return self.session.query(self.model).filter(self.model.is_subdag == False)

    def get_count_query(self):
        return self.session.query(func.count('*')).filter(self.model.is_subdag == False)

    @action('set_is_paused', "Set Pause", None)
    def action_set_is_paused(self, ids):
        self.set_dags(ids, "is_paused", True)

    @action('unset_is_paused', "Unset Pause", None)
    def action_unset_is_paused(self, ids):
        self.set_dags(ids, "is_paused", False)

    @action('set_is_active', "Set Active", None)
    def action_set_is_active(self, ids):
        self.set_dags(ids, "is_active", True)

    @action('unset_is_active', "Unset Active", None)
    def action_unset_is_active(self, ids):
        self.set_dags(ids, "is_active", False)

    @provide_session
    def set_dags(self, ids, key, value, session=None):
        try:
            count = 0
            for dag_model in session.query(self.model).filter(self.model.dag_id.in_(ids)).all():
                count += 1
                setattr(dag_model, key, value)
            session.commit()
            flash("{count} dag models '{key}' were set to '{value}'".format(**locals()))
        except Exception as ex:
            if not self.handle_view_exception(ex):
                raise Exception("Ooops")
            flash('Failed to set {key}'.format(**locals()), 'error')

    @csrf.exempt
    @expose("/api", methods=["GET", "POST"])
    @provide_session
    def api(self, session=None):
        api = request.args.get("api")
        if api == "get_dag_model_available_record":
            dag_model_available_records = session.query(DagModelAvailableRecord).order_by(DagModelAvailableRecord.created_at.desc()).limit(50)
            return jsonify([{
                "id": dag_model_available_record.id,
                "conf": dag_model_available_record.conf,
                "creator_user_id": dag_model_available_record.creator_user_id,
                "creator_user_name": dag_model_available_record.creator_user_name,
                "updated_at": dag_model_available_record.updated_at.strftime("%Y-%m-%d %H:%M:%S"),
                "created_at": dag_model_available_record.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            } for dag_model_available_record in dag_model_available_records])
        elif api == "checkout_dag_model_available_record":
            id = request.args.get("id")
            if id:
                dag_model_available_record = session.query(DagModelAvailableRecord).filter(DagModelAvailableRecord.id==id).first()
                if dag_model_available_record:
                    dag_model_available_record.restore(session)
                    session.commit()
                    return jsonify({"code": 0, "detail": "succeeded", })
                else:
                    return jsonify({"code": -2, "detail": "dag model available record does not exists", })
            else:
                return jsonify({"code": -1, "detail": "id required", })
        return jsonify({"code": -1000, "detail": "no such api", })


dag_available_view = DagAvailableView(DagAvailable, settings.Session, category="Browse", name="DAG Models")


dag_available_bp = Blueprint(
    "dag_available_bp",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/dag_available"
)


class DagAvailablePlugin(AirflowPlugin):
    name = "dag_available"
    operators = []
    flask_blueprints = [dag_available_bp]
    hooks = []
    executors = []
    admin_views = [dag_available_view]
    menu_links = []