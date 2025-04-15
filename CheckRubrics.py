import logging

from cerberus import Validator

log = logging.getLogger(__name__)

schema_rubric_in_database = {
    "_id": {"type": "string", "minlength": 4, "regex": r"^[\x21-\x7E]+$", "required": True},
    "_rev": {"type": "string", "required": True},
    "changed_by": {"type": "string", "nullable": True, "required": True},
    "changed_on": {"type": "integer", "min": 0, "nullable": True, "required": True},
    "created_by": {"type": "string", "required": True},
    "created_on": {"type": "integer", "min": 0, "required": True},
    "cyclic_transmit": {"type": "boolean", "required": True},
    "cyclic_transmit_interval": {"type": "integer", "min": 60, "max": 86400, "required": True},
    "priority": {"type": "integer", "min": 0, "max": 7, "required": True},
    "description": {"type": "string", "required": True},
    "expiration": {"type": "integer", "min": 60, "max": 86400, "required": True},
    "label": {"type": "string", "minlength": 1, "maxlength": 11, "regex": r"^[\x00-\x7F]*$", "required": True},
    "number": {"type": "integer", "min": 1, "max": 99, "required": True},
    "owners": {"type": "list", "schema": {"anyof": [{"type": "string"}]}, "required": True},
    "sub_ric": {"type": "integer", "min": 0, "max": 3, "required": True},
    "transmitter_groups": {
        "type": "list", "schema": {"anyof": [{"type": "string"}, {"nullable": True}]}, "required": True
    },
    "transmitters": {"type": "list", "schema": {"anyof": [{"type": "string"}, {"nullable": True}]}, "required": True},
}


def check_rubrics(couchdb_server, delete: bool):
    db_rubrics = couchdb_server["rubrics"]
    db_transmitters = couchdb_server["transmitters"]

    set_transmitter_ids = {
        member.key for member in db_transmitters.view("_design/transmitters/_view/id")
    }
    set_transmitter_groups = {
        member.key for member in db_transmitters.view("_design/transmitters/_view/groupstomembers")
    }

    for _id in db_rubrics:
        if _id == "_design/rubrics":
            continue
        dbo_rubric = db_rubrics[_id]

        val = Validator(schema_rubric_in_database)
        if not val.validate(dbo_rubric):
            log.error("rubric " + _id + " invalid: " + str(val.errors))
            continue
        log.debug("rubric " + _id + " valid")

        bool_has_valid_transmitter_groups = False
        bool_has_valid_transmitter_is = False

        for str_transmitter_group in dbo_rubric["transmitter_groups"]:
            if str_transmitter_group in set_transmitter_groups:
                bool_has_valid_transmitter_groups = True
            else:
                log.warning("rubric " + _id + " has unknown transmitter group " + str_transmitter_group)

        for str_transmitter in dbo_rubric["transmitters"]:
            if str_transmitter in set_transmitter_ids:
                bool_has_valid_transmitter_is = True
            else:
                log.warning("rubric " + _id + " has unknown transmitter " + str_transmitter)

        if not bool_has_valid_transmitter_groups and not bool_has_valid_transmitter_is:
            if delete:
                log.info("rubric " + _id + " has neither valid transmitter groups nor valid transmitters, deleting")
                db_rubrics.delete(dbo_rubric)
            else:
                log.info("rubric " + _id + " has neither valid transmitter groups nor valid transmitters, would delete")
