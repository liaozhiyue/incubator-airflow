# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from future.utils import native

import flask_login
from flask_login import login_required, current_user, logout_user
from flask import flash
from wtforms import (
    Form, PasswordField, StringField)
from wtforms.validators import InputRequired

from ldap3 import Server, Connection, Tls, LEVEL, SUBTREE, BASE
import ssl

from flask import url_for, redirect

from airflow import settings
from airflow import models
from airflow import configuration
from airflow.configuration import AirflowConfigException

import traceback
import re

# import urllib2
# from future.moves.urllib.parse import urljoin
# import requests
# from airflow.api.client import api_client

from airflow.utils.log.logging_mixin import LoggingMixin

login_manager = flask_login.LoginManager()
login_manager.login_view = 'airflow.login'  # Calls login() below
login_manager.login_message = None

log = LoggingMixin().log


class AuthenticationError(Exception):
    pass


class LdapException(Exception):
    pass

#
# def client_request(url, method='GET', json=None):
#     params = {
#         'url': url,
#         'auth': self._auth,
#     }
#     if json is not None:
#         params['json'] = json
#
#     resp = getattr(requests, method.lower())(**params)
#     if not resp.ok:
#         try:
#             data = resp.json()
#         except Exception:
#             data = {}
#         raise IOError(data.get('error', 'Server error'))
#
#     return resp.json()


def get_ldap_connection(dn=None, password=None):
    tls_configuration = None
    use_ssl = False
    try:
        cacert = configuration.get("ldap", "cacert")
        tls_configuration = Tls(validate=ssl.CERT_REQUIRED, ca_certs_file=cacert)
        use_ssl = True
    except:
        pass

    server = Server(configuration.get("ldap", "uri"), use_ssl, tls_configuration)
    conn = Connection(server, native(dn), native(password))

    if not conn.bind():
        log.error("Cannot bind to ldap server: %s ", conn.last_error)
        raise AuthenticationError("Cannot bind to ldap server")

    return conn


def group_contains_user(group, username):
    # base_url = 'https://airflow-dev.sre.gotokeep.com'
    # endpoint = '/admin/usergroup/api'

    # req = urllib2.Request(url)
    # print req
    # res_data = urllib2.urlopen(req)
    # res = res_data.read()
    #
    # data2 = r2.read()
    # conn.close()

    # url = urljoin(base_url, endpoint)
    # data = client_request(url, method='POST',
    #                      json={
    #                          "api": 'is_group_contains_user',
    #                          "username": username,
    #                          "group": group,
    #                      })

    dict = {
        'liaozhiyue': ['g_admin', 'g_warehouse'],
        'huangxuanfeng': ['g_warehouse'],
        'liyang': ['g_admin']
    }

    g = dict.get(username, [])

    return True if group in g else False


def groups_user(username):

    if username == 'liaozhiyue':
        groups = ['g_admin', 'g_warehouse']
    elif username == 'liyang':
        groups = ['g_admin']
    elif username == 'huangxuanfeng':
        groups = ['g_warehouse']
    else:
        groups =['g_guest']

    return groups


class LdapUser(models.User):
    def __init__(self, user):
        self.user = user
        self.ldap_groups = []

        # Load and cache superuser and data_profiler settings.
        conn = get_ldap_connection(configuration.get("ldap", "bind_user"),
                                   configuration.get("ldap", "bind_password"))

        superuser_filter = None
        data_profiler_filter = None
        try:
            superuser_filter = configuration.get("ldap", "superuser_filter")
        except AirflowConfigException:
            pass

        if not superuser_filter:
            self.superuser = True
            log.debug("Missing configuration for superuser settings or empty. Skipping.")
        else:
            admin_group = superuser_filter
            self.superuser = group_contains_user(admin_group, user.username)

        try:
            data_profiler_filter = configuration.get("ldap", "data_profiler_filter")
        except AirflowConfigException:
            pass

        if not data_profiler_filter:
            self.data_profiler = True
            log.debug("Missing configuration for data profiler settings or empty. "
                      "Skipping.")
        else:
            admin_group = data_profiler_filter
            self.data_profiler = group_contains_user(admin_group, user.username)

        # Load the ldap group(s) a user belongs to
        try:
            self.ldap_groups = groups_user(user.username)
        except AirflowConfigException:
            log.debug("Missing configuration for ldap settings. Skipping")

    @staticmethod
    def try_login(username, password):
        conn = get_ldap_connection(configuration.get("ldap", "bind_user"),
                                   configuration.get("ldap", "bind_password"))

        search_filter = "(&({0})({1}={2}))".format(
            configuration.get("ldap", "user_filter"),
            configuration.get("ldap", "user_name_attr"),
            username
        )

        search_scopes = {
            "LEVEL": LEVEL,
            "SUBTREE": SUBTREE,
            "BASE": BASE
        }

        search_scope = LEVEL
        if configuration.has_option("ldap", "search_scope"):
            search_scope = SUBTREE if configuration.get("ldap", "search_scope") == "SUBTREE" else LEVEL

        # todo: BASE or ONELEVEL?

        res = conn.search(native(configuration.get("ldap", "basedn")),
                          native(search_filter),
                          search_scope=native(search_scope))

        # todo: use list or result?
        if not res:
            log.info("Cannot find user %s", username)
            raise AuthenticationError("Invalid username or password")

        entry = conn.response[0]

        conn.unbind()

        if 'dn' not in entry:
            # The search filter for the user did not return any values, so an
            # invalid user was used for credentials.
            raise AuthenticationError("Invalid username or password")

        try:
            conn = get_ldap_connection(entry['dn'], password)
        except KeyError as e:
            log.error("""
            Unable to parse LDAP structure. If you're using Active Directory and not specifying an OU, you must set search_scope=SUBTREE in airflow.cfg.
            %s
            """ % traceback.format_exc())
            raise LdapException("Could not parse LDAP structure. Try setting search_scope in airflow.cfg, or check logs")

        if not conn:
            log.info("Password incorrect for user %s", username)
            raise AuthenticationError("Invalid username or password")

    def is_active(self):
        '''Required by flask_login'''
        return True

    def is_authenticated(self):
        '''Required by flask_login'''
        return True

    def is_anonymous(self):
        '''Required by flask_login'''
        return False

    def get_id(self):
        '''Returns the current user id as required by flask_login'''
        return self.user.get_id()

    def data_profiling(self):
        '''Provides access to data profiling tools'''
        return self.data_profiler

    def is_superuser(self):
        '''Access all the things'''
        return self.superuser


# 从cookie获取user_id后回调该方法反查user信息（符合一定条件的话user信息会提供给current_user变量使用）
@login_manager.user_loader
def load_user(userid):
    log.debug("Loading user %s", userid)
    if not userid or userid == 'None':
        return None

    session = settings.Session()
    user = session.query(models.User).filter(models.User.id == int(userid)).first()
    session.expunge_all()
    session.commit()
    session.close()
    return LdapUser(user)


# login_manager.login_view = 'airflow.login'. 由flask_login 框架调用
def login(self, request):
    if current_user.is_authenticated():
        flash("You are already logged in")
        return redirect(url_for('admin.index'))

    username = None
    password = None

    form = LoginForm(request.form)

    if request.method == 'POST' and form.validate():
        username = request.form.get("username")
        password = request.form.get("password")

    if not username or not password:
        return self.render('airflow/login.html',
                           title="Airflow - Login",
                           form=form)

    try:
        # 验证登陆
        LdapUser.try_login(username, password)
        log.info("User %s successfully authenticated", username)

        # 更新已有用户/新建用户到数据库表
        session = settings.Session()
        user = session.query(models.User).filter(
            models.User.username == username).first()

        if not user:
            user = models.User(
                username=username,
                is_superuser=False)

        session.merge(user)
        session.commit()

        # 提交用户信息，写入cookie（记录user_id，使用时用@login_manager.user_loader回调方法反查User信息）
        flask_login.login_user(LdapUser(user))

        # 提交数据库session并关闭
        session.commit()
        session.close()

        return redirect(request.args.get("next") or url_for("admin.index"))
    except (LdapException, AuthenticationError) as e:
        if type(e) == LdapException:
            flash(e, "error")
        else:
            flash("Incorrect login details")
        return self.render('airflow/login.html',
                           title="Airflow - Login",
                           form=form)


class LoginForm(Form):
    username = StringField('Username', [InputRequired()])
    password = PasswordField('Password', [InputRequired()])
