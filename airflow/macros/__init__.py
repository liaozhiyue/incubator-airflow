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

from __future__ import absolute_import
from datetime import datetime, timedelta
import dateutil
import time
from . import hive
import uuid


def ds_add(ds, days):
    """
    Add or subtract days from a YYYY-MM-DD

    :param ds: anchor date in ``YYYY-MM-DD`` format to add to
    :type ds: str
    :param days: number of days to add to the ds, you can use negative values
    :type days: int

    >>> ds_add('2015-01-01', 5)
    '2015-01-06'
    >>> ds_add('2015-01-06', -5)
    '2015-01-01'
    """

    ds = datetime.strptime(ds, '%Y-%m-%d')
    if days:
        ds = ds + timedelta(days)
    return ds.isoformat()[:10]


def ds_add_hours(ds, hours = 8):
    """
        Add or subtract hours from a YYYY-MM-DD %H:%M:%S

        :param ds: anchor date in ``YYYY-MM-DD %H:%M:%S`` format to add to
        :type ds: datetime
        :param hours: number of hours to add to the ds, you can use negative values,default = 8
        :type hours: int

        >>> ds_add_hours('2015-01-01 12:23:32', 8)
        '2015-01-01 20:23:32'
        >>> ds_add_hours('2015-01-06 20:32:12', 8)
        '2015-01-07 04:32:12'
    """
    ds = (ds + timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
    return ds

def to_Beijing_date(ds, format = '%Y-%m-%d'):
    """
        Takes an string  and format string as specified in the output format
        :param ds: input string which contains a date
        :type ds: str
        :param format: output string format. default %Y-%m-%d
        :type format: str
        >>> to_Beijing_date('2015-01-01 12:23:32')
        '2015-01-01'
        >>> to_Beijing_date('2015-01-06 20:32:12', '%Y%m%d')
        '20150106'
        dags:
            t4 = BashOperator(
                task_id='shell',
                bash_command='echo ' + '{{ macros.to_Beijing_date(execution_date) }}',dag=dag)
    """
    ds = ds_add_hours(ds)
    if format.find('-') == -1:
        if ds.find('-') > -1:
            ds = ds.replace('-','')
    if ds.find(' ') > -1:
        ds = ds.split(' ')[0]
    return datetime.strptime(ds, format).strftime(format)



def ds_format(ds, input_format, output_format):
    """
    Takes an input string and outputs another string
    as specified in the output format

    :param ds: input string which contains a date
    :type ds: str
    :param input_format: input string format. E.g. %Y-%m-%d
    :type input_format: str
    :param output_format: output string format  E.g. %Y-%m-%d
    :type output_format: str

    >>> ds_format('2015-01-01', "%Y-%m-%d", "%m-%d-%y")
    '01-01-15'
    >>> ds_format('1/5/2015', "%m/%d/%Y",  "%Y-%m-%d")
    '2015-01-05'
    """
    return datetime.strptime(ds, input_format).strftime(output_format)


def _integrate_plugins():
    """Integrate plugins to the context"""
    import sys
    from airflow.plugins_manager import macros_modules
    for macros_module in macros_modules:
        sys.modules[macros_module.__name__] = macros_module
        globals()[macros_module._name] = macros_module

        ##########################################################
        # TODO FIXME Remove in Airflow 2.0

        import os as _os
        if not _os.environ.get('AIRFLOW_USE_NEW_IMPORTS', False):
            from zope.deprecation import deprecated as _deprecated
            for _macro in macros_module._objects:
                macro_name = _macro.__name__
                globals()[macro_name] = _macro
                _deprecated(
                    macro_name,
                    "Importing plugin macro '{i}' directly from "
                    "'airflow.macros' has been deprecated. Please "
                    "import from 'airflow.macros.[plugin_module]' "
                    "instead. Support for direct imports will be dropped "
                    "entirely in Airflow 2.0.".format(i=macro_name))
