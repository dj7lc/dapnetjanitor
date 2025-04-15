import logging
import time

from cerberus import Validator

log = logging.getLogger(__name__)

schema_news_in_database = {
    "_id": {"type": "string", "regex": r"^[\x00-\x7F]*$", "required": True},
    "_rev": {"type": "string", "required": True},
    "data": {"type": "list", "minlength": 10, "maxlength": 10, "schema": {"type": "string"}},
    "time": {"type": "list", "minlength": 10, "maxlength": 10, "schema": {"type": "integer"}},
}


def check_news(couchdb_server, delete: bool):
    db_news = couchdb_server["news"]
    db_rubrics = couchdb_server["rubrics"]

    for _id in db_news:
        if _id == "_design/news":
            continue
        dbo_news = db_news[_id]

        val = Validator(schema_news_in_database)
        if not val.validate(dbo_news):
            log.error(f"news {_id} invalid: {val.errors}")
            continue
        log.debug(f"news {_id} valid")

        if _id not in db_rubrics:
            if delete:
                log.info(f"rubric for news {_id} does not exist, deleting")
                db_news.delete(dbo_news)
                continue
            else:
                log.info(f"rubric for news {_id} does not exist, would delete")

        for index in range(0, 10):
            if int(time.time()) - dbo_news["time"][index] > db_rubrics[_id]["expiration"]:
                if delete:
                    log.info(f"news {_id} entry {index} has expired, deleting")
                    dbo_news["data"][index] = ""
                    dbo_news["time"][index] = 0
                else:
                    log.info(f"news {_id} entry {index} has expired, would delete")
            else:
                log.debug(f"news {_id} entry {index} not expired, do not delete")

        if dbo_news["time"] == [0] * 10:
            if delete:
                log.info(f"news {_id} has no data left, deleting")
                db_news.delete(dbo_news)
                continue
            else:
                log.info(f"news {_id} has no data left, would delete")
                continue
        else:
            log.debug(f"news {_id} has non-expired data, do not delete")

        db_news.save(dbo_news)
