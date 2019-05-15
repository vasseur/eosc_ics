import os
import json
import logging

from typing import List, Dict
from airflow import settings
from airflow.models import Connection

log = logging.getLogger(__name__)
here = os.path.dirname(os.path.abspath(__file__))


def _get_connection_data(conn_id: str, workflow: str, entry: Dict) -> Dict:
    """ Data to create Connections objects """

    return dict(
        conn_id=conn_id,
        conn_type="FTP",
        host=entry["host"],
        login=entry["login"],
        password=entry["password"],
        port=entry["port"],
        extra=json.dumps(
            {
                "source_directory": entry["directory"],
                "target_format": entry["target_format"],
                "pipeline": entry["pipeline"],
                "collection": workflow,
            }
        ),
    )


def create_connections() -> None:
    """ Create entries in connection based on assets/connections.json.
        This is done once, at launch time.
        NB: should we move the data in a remote mongo ?
    """

    session = settings.Session()
    with open(os.path.join(here, "../assets/connections.json"), "r") as f:
        data = json.loads(f.read())
        for workflow in data:
            for entry in data[workflow]:
                conn_id = "{}__{}".format(workflow, entry["name"])
                session.query(Connection).filter_by(conn_id=conn_id).delete()
                conn = Connection(**_get_connection_data(conn_id, workflow, entry))
                session.add(conn)
    session.commit()


def get_connections(collection: str) -> List[str]:
    """ Get all connections ids in a collection.
        Collections are defined by their name.
    """

    session = settings.Session()
    return [
        c
        for c in session.query(Connection).filter(
            Connection.conn_id.ilike("{}__%s".format(collection))
        )
    ]


def get_connection(conn_id: str) -> Connection:
    """ Get connection by id """

    return settings.Session().query(Connection).filter_by(conn_id=conn_id).one()
