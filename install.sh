echo 'uninstall apache-airflow'
pip uninstall apache-airflow
echo 'install apache-airflow'
python setup.py install

current_date=`date "+%Y%m%d%H%m%S"`
installed_path=${AIRFLOW_HOME}/plugins
source_path=/disk/m/airflow-code/airflow
backup_path=${AIRFLOW_HOME}/plugins.bak.${current_date}

echo "backup ${installed_path} to ${backup_path}"
mv ${installed_path} ${backup_path}
mkdir ${installed_path}
echo "install plugins: cp to ${source_path}/airflow-dag-creation-manager-plugin/plugins/dcmp to ${installed_path}/"
cp -r ${source_path}/airflow-dag-creation-manager-plugin/plugins/dcmp ${installed_path}/ 
echo "install plugins: cp to ${source_path}/airflow-dag-status-manager-plugin/plugins/dag_available_plugin to ${installed_path}/"
cp -r ${source_path}/airflow-dag-status-manager-plugin/plugins/dag_available_plugin ${installed_path}/
echo "install plugins: cp to ${source_path}/airflow-user-group-manager-plugin/plugins/ugmp to ${installed_path}/"
cp -r ${source_path}/airflow-user-group-manager-plugin/plugins/ugmp ${installed_path}/
