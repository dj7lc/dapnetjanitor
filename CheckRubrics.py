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
        "type": "list",
        "schema": {"anyof": [{"type": "string"}, {"nullable": True}]},
        "required": True,
    },
    "transmitters": {"type": "list", "schema": {"anyof": [{"type": "string"}, {"nullable": True}]}, "required": True},
}


def check_rubrics(couchdb_server, delete: bool):
    db_rubrics = couchdb_server["rubrics"]
    db_transmitters = couchdb_server["transmitters"]

    set_transmitter_ids = {member.key for member in db_transmitters.view("_design/transmitters/_view/id")}
    set_transmitter_groups = {
        member.key for member in db_transmitters.view("_design/transmitters/_view/groupstomembers")
    }

    for _id in db_rubrics:
        if _id == "_design/rubrics":
            continue
        dbo_rubric = db_rubrics[_id]

        val = Validator(schema_rubric_in_database)
        if not val.validate(dbo_rubric):
            log.error(f"rubric {_id} invalid: {val.errors}")
            continue
        log.debug(f"rubric {_id} valid")

        set_transmitter_groups_of_entry = set(dbo_rubric["transmitter_groups"])
        set_transmitter_ids_of_entry = set(dbo_rubric["transmitters"])

        set_valid_transmitter_groups = set_transmitter_groups_of_entry & set_transmitter_groups
        set_valid_transmitters = set_transmitter_ids_of_entry & set_transmitter_ids

        set_invalid_transmitter_groups = set_transmitter_groups_of_entry - set_valid_transmitter_groups
        set_invalid_transmitters = set_transmitter_ids_of_entry - set_valid_transmitters

        if not set_valid_transmitter_groups and not set_valid_transmitters:
            if delete:
                log.info(f"rubric {_id} has neither valid transmitter groups nor valid transmitters, deleting")
                db_rubrics.delete(dbo_rubric)
            else:
                log.info(f"rubric {_id} has neither valid transmitter groups nor valid transmitters, would delete")
            continue

        if set_invalid_transmitter_groups:
            if delete:
                log.info(f"rubric {_id} has has invalid transmitter groups {set_invalid_transmitter_groups}, updating")
                dbo_rubric["transmitter_groups"] = list(set_valid_transmitter_groups)
            else:
                log.info(
                    f"rubric {_id} has has invalid transmitter groups {set_invalid_transmitter_groups}, would update"
                )

        if set_invalid_transmitters:
            if delete:
                log.info(f"rubric {_id} has has invalid transmitters {set_invalid_transmitters}, updating")
                dbo_rubric["transmitters"] = list(set_valid_transmitters)
            else:
                log.info(f"rubric {_id} has has invalid transmitters {set_invalid_transmitters}, would update")
