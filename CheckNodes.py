import logging
import time

from cerberus import Validator

log = logging.getLogger(__name__)


def validate_coordinates(field_designator, value, error):
    if not (
        isinstance(value, list)
        and len(value) == 2
        and isinstance(value[0], float)
        and isinstance(value[1], float)
        and abs(value[0]) <= 90
        and abs(value[1]) <= 180
    ):
        error(
            field_designator,
            "coordinates must be a list of two floats, latitude must be in [-90, 90], longitude must be in [-180, 180]",
        )


schema_node_in_database = {
    "_id": {"type": "string", "minlength": 4, "regex": r"^[\x21-\x7E]+$", "required": True},
    "_rev": {"type": "string", "required": True},
    "auth_key": {"type": "string", "regex": r"^[0-9A-Za-z]{16,32}$", "required": True},
    "changed_by": {"type": "string", "nullable": True, "required": True},
    "changed_on": {"type": "integer", "min": 0, "nullable": True, "required": True},
    "coordinates": {"type": "list", "validator": validate_coordinates, "required": True},
    "created_by": {"type": "string", "required": True},
    "created_on": {"type": "integer", "min": 0, "required": True},
    "description": {"type": "string", "nullable": True, "required": True},
    "hostname_hamnet": {"type": "string", "required": True},
    "hostname_internet": {"type": "string", "nullable": True, "required": True},
    "owners": {"type": "list", "schema": {"type": "string"}, "required": True},
}


def check_nodes(couchdb_server, delete: bool):
    db_nodes = couchdb_server["nodes"]

    for _id in db_nodes:
        if _id == "_design/nodes":
            continue

        dbo_node = db_nodes[_id]

        val = Validator(schema_node_in_database)
        if not val.validate(dbo_node):
            log.error(f"node {_id} invalid: {val.errors}")
            continue
        log.debug(f"node {_id} valid")
