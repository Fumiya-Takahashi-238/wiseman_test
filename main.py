# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import logging
import os

from flask import Flask, request, Response, jsonify
import sqlalchemy
from flask import flash, redirect, url_for
from werkzeug.utils import secure_filename

ALLOWED_EXTENSIONS = {'txt', 'pdf', 'csv'}


app = Flask(__name__)

logger = logging.getLogger()


def init_connection_engine():
    db_config = {
        # [START cloud_sql_postgres_sqlalchemy_limit]
        # Pool size is the maximum number of permanent connections to keep.
        "pool_size": 5,
        # Temporarily exceeds the set pool_size if no connections are available.
        "max_overflow": 2,
        # The total number of concurrent connections for your application will be
        # a total of pool_size and max_overflow.
        # [END cloud_sql_postgres_sqlalchemy_limit]

        # [START cloud_sql_postgres_sqlalchemy_backoff]
        # SQLAlchemy automatically uses delays between failed connection attempts,
        # but provides no arguments for configuration.
        # [END cloud_sql_postgres_sqlalchemy_backoff]

        # [START cloud_sql_postgres_sqlalchemy_timeout]
        # 'pool_timeout' is the maximum number of seconds to wait when retrieving a
        # new connection from the pool. After the specified amount of time, an
        # exception will be thrown.
        "pool_timeout": 30,  # 30 seconds
        # [END cloud_sql_postgres_sqlalchemy_timeout]

        # [START cloud_sql_postgres_sqlalchemy_lifetime]
        # 'pool_recycle' is the maximum number of seconds a connection can persist.
        # Connections that live longer than the specified amount of time will be
        # reestablished
        "pool_recycle": 1800,  # 30 minutes
        # [END cloud_sql_postgres_sqlalchemy_lifetime]
    }

    # if os.environ.get("DB_HOST"):
    #     return init_tcp_connection_engine(db_config)
    # else:
    #     return init_unix_connection_engine(db_config)
    return init_unix_connection_engine(db_config)


def init_tcp_connection_engine(db_config):
    # [START cloud_sql_postgres_sqlalchemy_create_tcp]
    # Remember - storing secrets in plaintext is potentially unsafe. Consider using
    # something like https://cloud.google.com/secret-manager/docs/overview to help keep
    # secrets secret.
    db_user = os.environ["DB_USER"]
    db_pass = os.environ["DB_PASS"]
    db_name = os.environ["DB_NAME"]
    db_host = os.environ["DB_HOST"]

    # Extract host and port from db_host
    host_args = db_host.split(":")
    db_hostname, db_port = host_args[0], int(host_args[1])

    pool = sqlalchemy.create_engine(
        # Equivalent URL:
        # postgres+pg8000://<db_user>:<db_pass>@<db_host>:<db_port>/<db_name>
        sqlalchemy.engine.url.URL(
            drivername="postgres+pg8000",
            username=db_user,  # e.g. "my-database-user"
            password=db_pass,  # e.g. "my-database-password"
            host=db_hostname,  # e.g. "127.0.0.1"
            port=db_port,  # e.g. 5432
            database=db_name  # e.g. "my-database-name"
        ),
        # ... Specify additional properties here.
        # [END cloud_sql_postgres_sqlalchemy_create_tcp]
        **db_config
        # [START cloud_sql_postgres_sqlalchemy_create_tcp]
    )
    # [END cloud_sql_postgres_sqlalchemy_create_tcp]

    return pool


def init_unix_connection_engine(db_config):
    # [START cloud_sql_postgres_sqlalchemy_create_socket]
    # Remember - storing secrets in plaintext is potentially unsafe. Consider using
    # something like https://cloud.google.com/secret-manager/docs/overview to help keep
    # secrets secret.
    db_user = os.environ["DB_USER"]
    db_pass = os.environ["DB_PASS"]
    db_name = os.environ["DB_NAME"]
    db_socket_dir = os.environ.get("DB_SOCKET_DIR", "/cloudsql")
    cloud_sql_connection_name = os.environ["CLOUD_SQL_CONNECTION_NAME"]
    
    pool = sqlalchemy.create_engine(
        # Equivalent URL:
        # postgres+pg8000://<db_user>:<db_pass>@/<db_name>
        #                         ?unix_sock=<socket_path>/<cloud_sql_instance_name>/.s.PGSQL.5432
        sqlalchemy.engine.url.URL(
            drivername="postgres+pg8000",
            username=db_user,  # e.g. "my-database-user"
            password=db_pass,  # e.g. "my-database-password"
            database=db_name,  # e.g. "my-database-name"
            query={
                "unix_sock": "{}/{}/.s.PGSQL.5432".format(
                    db_socket_dir,  # e.g. "/cloudsql"
                    cloud_sql_connection_name)  # i.e "<PROJECT-NAME>:<INSTANCE-REGION>:<INSTANCE-NAME>"
            }
        ),
        # ... Specify additional properties here.
        # [END cloud_sql_postgres_sqlalchemy_create_socket]
        **db_config
        # [START cloud_sql_postgres_sqlalchemy_create_socket]
    )
    # [END cloud_sql_postgres_sqlalchemy_create_socket]

    return pool
    # except sqlalchemy.exc.DBAPIError:
    #     return jsonify({"message": "Connection was invalidated!"})


# The SQLAlchemy engine will help manage interactions, including automatically
# managing a pool of connections to your database
db_engine = init_connection_engine()

# try:
#     # suppose the database has been restarted.
#     c = db.connect()
#     c.execute("CREATE TABLE IF NOT EXISTS test(id INTEGER PRIMARY KEY, name TEXT)")
#     c.execute("SELECT * FROM test")
#     c.close()
# except sqlalchemy.exc.DBAPIError as db:
#     # an exception is raised, Connection is invalidated.
#     if db.connection_invalidated:
#         print("Connection was invalidated!")


@app.before_first_request
def create_tables():
    # Create tables (if they don't already exist)
    with db_engine.connect() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS test(id INTEGER PRIMARY KEY, name TEXT)")
        #conn.execute("INSERT INTO test VALUES (1,'fukuma')")
        #conn.execute("INSERT INTO test VALUES (2,'takahashi')")


# @app.route('/')
# def hello():
#     """Return a friendly HTTP greeting."""
#     return 'Hello World!'


@app.route('/', methods=['GET'])
def index():
    try:
        test = []

        with db_engine.connect() as conn:
            # Execute the query and fetch all results
            #conn.execute("INSERT INTO  TABLE test_wiseman VALUES ('fukuma')")

            recent_test = conn.execute(
                "SELECT * FROM test"
            ).fetchall()
            # Convert the results into a list of dicts representing test
            for row in recent_test:
                test.append({'id': row[0], 'name': row[1]})
    except sqlalchemy.exc.DBAPIError as db:
        # an exception is raised, Connection is invalidated.
        if db.connection_invalidated:
            test = "Connection was invalidated!"

    return jsonify({"member": test})


@app.route('/store_data', methods=['POST'])
def save_newcomer():
    # Get the team and time the vote was cast.
    new_comer = request.json
    id = new_comer["id"]
    name = new_comer["name"]
    # [START cloud_sql_postgres_sqlalchemy_connection]
    # Preparing a statement before hand can help protect against injections.
    stmt = sqlalchemy.text(
        "INSERT INTO test (id, name)"
        " VALUES (:id, :name)"
    )
    try:
        # Using a with statement ensures that the connection is always released
        # back into the pool at the end of statement (even if an error occurs)
        with db_engine.connect() as conn:
            conn.execute(stmt, id=id, name=name)
    except Exception as e:
        # If something goes wrong, handle the error in this section. This might
        # involve retrying or adjusting parameters depending on the situation.
        # [START_EXCLUDE]
        logger.exception(e)
        return Response(
            status=500,
            response="Unable to successfully cast vote! Please check the "
                     "application logs for more details."
        )
        # [END_EXCLUDE]
    # [END cloud_sql_postgres_sqlalchemy_connection]

    return Response(
        status=200,
        response="Vote successfully cast for '{}' at time {}!".format(
            name, id)
    )


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/load_csv', methods=['GET', 'POST'])
def load_csv():
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            # file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            # return redirect(url_for('uploaded_file',
            #                         filename=filename))
            return jsonify({"message": f"{filename} found"})
    return '''
    <!doctype html>
    <title>Upload new File</title>
    <h1>Upload new File</h1>
    <form method=post enctype=multipart/form-data>
      <input type=file name=file>
      <input type=submit value=Upload>
    </form>
    '''


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)

# @app.route('/', method = ['POST'])
# def get_reliability(user_id, passward):
#     return 

# @app.route('/get_reliability', method = ['GET'])
# def get_reliability(item_id):

#     return 

# @app.route('/dataset', method = ['POST'])
# def dataset(user_id, data):
#     try:

#     except:
    
#     return

# @app.route('/dataset', method = ['POST'])
# def create_model(dataset_id):
    
#     return
