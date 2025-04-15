import logging
import time

from cerberus import Validator

log = logging.getLogger(__name__)

schema_call_in_database = {
    "_id": {"type": "string", "regex": r"^[\x00-\x7F]*$", "required": True},
    "_rev": {"type": "string", "required": True},
    "created_by": {"type": "string", "required": True},
    "created_on": {"type": "integer", "min": 0, "required": True},
    "data": {"type": "string", "maxlength": 160, "required": True},
    "expiration": {"type": "integer", "min": 60, "max": 86400, "required": True},
    "local": {"type": "boolean", "required": True},
    "origin": {"type": "string", "maxlength": 160, "required": True},
    "priority": {"type": "integer", "min": 0, "max": 7, "required": True},
    "subscriber_groups": {"type": "list", "schema": {"anyof": [{"type": "string"}, {"nullable": True}]}},
    "subscribers": {"type": "list", "schema": {"anyof": [{"type": "string"}, {"nullable": True}]}},
    "transmitter_groups": {"type": "list", "schema": {"anyof": [{"type": "string"}, {"nullable": True}]}},
    "transmitters": {"type": "list", "schema": {"anyof": [{"type": "string"}, {"nullable": True}]}},
}


def check_calls(couchdb_server, delete: bool):
    db_calls = couchdb_server["calls"]

    for _id in db_calls:
        if _id == "_design/calls":
            continue
        dbo_call = db_calls[_id]

        val = Validator(schema_call_in_database)
        if not val.validate(dbo_call):
            log.error("call " + _id + " invalid: " + str(val.errors))
            if delete:
                log.error("call " + _id + " invalid, deleting")
                db_calls.delete(dbo_call)
            else:
                log.error("call " + _id + " invalid, would delete")
            continue
        log.debug("call " + _id + " valid")

        if dbo_call["expiration"] + dbo_call["created_on"] - int(time.time()) < 0:
            if delete:
                log.info("call " + _id + " expired, deleting")
                db_calls.delete(dbo_call)
            else:
                log.info("call " + _id + " expired, would delete")
        else:
            log.debug("call " + _id + " not expired, do not delete")
