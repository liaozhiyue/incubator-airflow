# encoding: utf-8

import airflow
from airflow import settings, configuration
from airflow.plugins_manager import AirflowPlugin
from airflow.www import utils as wwwutils
from airflow.www.app import csrf
from airflow.utils.db import provide_session
from flask import Blueprint, request, jsonify
from flask_login import flash
from flask_admin import BaseView, expose
from flask_admin.contrib.sqla import ModelView
from flask_admin.contrib.sqla.view import func
from flask_admin.actions import action
from functools import wraps

from ugmp.models import UserGroup


def login_required(func):
# when airflow loads plugins, login is still None.
    @wraps(func)
    def func_wrapper(*args, **kwargs):
        if airflow.login:
            return airflow.login.login_required(func)(*args, **kwargs)
        return func(*args, **kwargs)
    return func_wrapper

class AirflowModelView(ModelView):
    list_template = 'dag_available_plugin/model_list.html'
    edit_template = 'airflow/model_edit.html'
    create_template = 'airflow/model_create.html'
    column_display_actions = True
    page_size = 500


class UserGroupView(wwwutils.SuperUserMixin, AirflowModelView):
    verbose_name = "UserGroup Model"
    verbose_name_plural = "UserGroup Models"
    column_default_sort = 'id'
    can_create = False
    can_delete = False
    can_edit = False
    column_display_actions = False
    column_list = ('id', 'username', 'group', 'creator_user_name', 'updated_at', 'created_at',)
    column_filters = ('username', 'group', 'creator_user_name',)
    #form_columns = ('is_active', 'is_paused', )

    #def get_query(self):
    #    return self.session.query(self.model).filter(self.model.is_subdag == False)

    #def get_count_query(self):
    #    return self.session.query(func.count('*')).filter(self.model.is_subdag == False)

    @csrf.exempt
    @expose("/api", methods=["GET", "POST"])
    @login_required
    @provide_session
    def api(self, session=None):
        api = request.args.get("api")
        # 按照用户查找用户组
        if api == "get_groups_by_user":
            username = request.args.get("username")
            if username:
                user_groups = session.query(UserGroup).filter(
                    UserGroup.username == username
                ).order_by(
                    UserGroup.created_at.desc()
                ).limit(200)
                return jsonify([{
                    "id": user_group.id,
                    "username": user_group.username,
                    "group": user_group.group,
                    "creator_user_id": user_group.creator_user_id,
                    "creator_user_name": user_group.creator_user_name,
                    "updated_at": user_group.updated_at.strftime("%Y-%m-%d %H:%M:%S"),
                    "created_at": user_group.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                } for user_group in user_groups])
            else:
                return jsonify({"code": -1, "detail": "username required", })
        # 用户是否在组里面？
        elif api == "is_group_contains_user":
            username = request.args.get("username")
            group = request.args.get("group")
            if username and group:
                user_groups = session.query(UserGroup).filter(
                    UserGroup.group == group
                ).filter(
                    UserGroup.username == username
                )
                return jsonify({
                    "contains": True if len(user_groups) != 0 else False
                })
            else:
                return jsonify({"code": -1, "detail": "username and group required", })
        elif api == "get_user_group":
            user_groups = session.query(UserGroup).order_by(UserGroup.created_at.desc()).limit(200)
            return jsonify([{
                "id": user_group.id,
                "username": user_group.username,
                "group": user_group.group,
                "creator_user_id": user_group.creator_user_id,
                "creator_user_name": user_group.creator_user_name,
                "updated_at": user_group.updated_at.strftime("%Y-%m-%d %H:%M:%S"),
                "created_at": user_group.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            } for user_group in user_groups])
        return jsonify({"code": -1, "detail": "no such api", })


user_group_view = UserGroupView(UserGroup, settings.Session, category="Admin", name="User Group Manager")


user_group_bp = Blueprint(
    "user_group_bp",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/user_group"
)


class UserGroupManagerPlugin(AirflowPlugin):
    name = "user_group_manager"
    operators = []
    flask_blueprints = [user_group_bp]
    hooks = []
    executors = []
    admin_views = [user_group_view]
    menu_links = []
