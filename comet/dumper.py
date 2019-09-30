"""Dump JSON to files."""

import asyncio
import datetime

import json
import os
from sanic.log import logger

from .manager import TIMESTAMP_FORMAT

FILENAME = "{}-data.dump"


class Dumper:
    """Take JSON, write file."""

    def __init__(self, path, file_lock_time):
        if not os.path.isdir(path):
            logger.error(
                "dump-path '{}' does not exist or is not a directory.".format(path)
            )
            exit(1)
        self.path = path
        self.lock_time = datetime.timedelta(seconds=file_lock_time)
        self.open_file = None
        self.open_time = None
        self.lock_file = None
        self.lock = asyncio.Lock()

    def __del__(self):
        """
        Destruct a dumper.

        Remove any lock files.
        """
        if self.lock_file is not None:
            os.remove(self.lock_file)

    async def _new_file(self):
        # Unlock the old file
        if self.lock_file is not None:
            os.remove(self.lock_file)

        # Create new file
        now = datetime.datetime.utcnow()
        self.open_time = now
        self.open_file = os.path.join(
            self.path, FILENAME.format(now.strftime(TIMESTAMP_FORMAT))
        )
        self.lock_file = "{}.lock".format(self.open_file)
        open(self.lock_file, "x").close()
        open(self.open_file, "x").close()

        logger.debug("Created new file: {}".format(self.open_file))

    async def dump(self, data):
        """
        Dump json to file.

        Parameters
        ----------
        data : json
            JSON object to dump.
        """
        # Make sure the dump has a timestamp
        if "time" not in data.keys():
            data["time"] = datetime.datetime.utcnow().strftime(TIMESTAMP_FORMAT)

        async with self.lock:
            if self.open_time is None:
                await self._new_file()
            elif datetime.datetime.utcnow() - self.open_time > self.lock_time:
                await self._new_file()

            # Dump to file
            with open(self.open_file, "a") as dump_file:
                json.dump(data, dump_file)
                dump_file.write("\n")
                dump_file.flush()
