import os
import json
import logging

from typing import List
from airflow import settings
from airflow.models import Connection

log = logging.getLogger(__name__)
here = os.path.dirname(os.path.abspath(__file__))


def create_connections():
    """ Create entries in connection based on assets/connections.json.
        This is done once, at launch time.
        NB: should we move the data in a remote mongo ?
    """

    session = settings.Session()
    with open(os.path.join(here, "../assets/connections.json"), "r") as f:
        data = json.loads(f.read())
        for workflow in data:
            for entry in data[workflow]["sites"]:
                conn_id = "{}__{}".format(workflow, entry["name"])
                session.query(Connection).filter_by(conn_id=conn_id).delete()
                conn = Connection(
                    conn_id=conn_id,
                    conn_type="FTP",
                    host=entry["host"],
                    login=entry["login"],
                    password=entry["password"],
                    port=entry["port"],
                    extra=json.dumps(
                        {
                            "source_directory": entry["directory"],
                            "source_orientation": entry["orientation"],
                            "target_format": data[workflow]["target"]["format"],
                            "target_orientation": data[workflow]["target"]["orientation"],
                            "target_colorscale": data[workflow]["target"]["colorscale"]
                        }),
                )
                session.add(conn)
    session.commit()


def get_connections(collection: str) -> List[str]:
    """ Get all connections ids in a collection.
        Collections are defined by their name.
    """

    session = settings.Session()
    return [
        c.conn_id
        for c in session.query(Connection).filter(
            Connection.conn_id.ilike("{}__%s".format(collection))
        )
    ]


def get_connection(conn_id: str) -> Connection:
    """ Get connection by id """

    return settings.Session().query(Connection).filter_by(conn_id=conn_id).one()