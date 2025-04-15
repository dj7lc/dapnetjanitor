import logging

from cerberus import Validator

log = logging.getLogger(__name__)

schema_subscriber_in_database = {
    "_id": {"type": "string", "minlength": 4, "regex": r"^[\x21-\x7E]+$", "required": True},
    "_rev": {"type": "string", "required": True},
    "changed_by": {"type": "string", "nullable": True, "required": True},
    "changed_on": {"type": "integer", "min": 0, "nullable": True, "required": True},
    "created_by": {"type": "string", "required": True},
    "created_on": {"type": "integer", "min": 0, "required": True},
    "description": {"type": "string", "required": True},
    "groups": {"type": "list", "schema": {"anyof": [{"type": "string"}, {"nullable": True}]}, "required": True},
    "owners": {"type": "list", "schema": {"anyof": [{"type": "string"}]}, "required": True},
    "pagers": {
        "type": "list",
        "schema": {
            "type": "dict",
            "schema": {
                "enabled": {"type": "boolean", "required": True},
                "name": {"type": "string", "required": True},
                "ric": {"type": "integer", "min": 6000, "max": 2097153, "required": True},
                "sub_ric": {"type": "integer", "min": 0, "max": 3, "required": True},
                "type": {"type": "string", "allowed": ["swissphone", "skyper", "alphapoc", "tpl"], "required": True},
            },
        },
    },
    "third_party_services": {
        "type": "dict",
        "schema": {
            "aprs": {"type": "list", "schema": {"anyof": [{"type": "string"}, {"nullable": True}]}},
            "brandmeister": {
                "type": "list",
                "schema": {"anyof": [{"type": "integer", "min": 1000000, "max": 10000000}, {"nullable": True}]},
            },
            "email": {
                "type": "list",
                "schema": {
                    "anyof": [
                        {"type": "string", "regex": r"^[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w+$"},
                        {"nullable": True},
                    ]
                },
            },
            "hamstatus_fms": {"type": "list", "schema": {"anyof": [{"type": "string"}, {"nullable": True}]}},
            "ipsc2": {
                "type": "list",
                "schema": {"anyof": [{"type": "integer", "min": 1000000, "max": 10000000}, {"nullable": True}]},
            },
            "meshcom": {"type": "list", "schema": {"anyof": [{"type": "string"}, {"nullable": True}]}},
            "telegram": {"type": "string", "regex": r"^[0-9A-Za-z]{40}$", "nullable": True},
            "tmo_services": {
                "type": "list",
                "schema": {"anyof": [{"type": "integer", "min": 1000000, "max": 10000000}, {"nullable": True}]},
            },
        },
    },
}


def check_subscribers(couchdb_server, delete: bool):
    db_subscribers = couchdb_server["subscribers"]
    db_transmitters = couchdb_server["transmitters"]

    for _id in db_subscribers:
        if _id == "_design/subscribers":
            continue
        dbo_subscriber = db_subscribers[_id]

        val = Validator(schema_subscriber_in_database)
        if not val.validate(dbo_subscriber):
            log.error("subscriber " + _id + " invalid: " + str(val.errors))
            continue
        log.debug("subscriber " + _id + " valid")
