"""
Archiver for CoMeT.

Moves comet broker dumps to a database
"""

import asyncio
import datetime
import orjson as json
from pathlib import Path
import signal

import logging

import chimedb.core as chimedb
import chimedb.dataset as db

from . import Manager, CometError, __version__
from .broker import DEFAULT_PORT
from .manager import TIMESTAMP_FORMAT, LOG_FORMAT


logging.basicConfig(format=LOG_FORMAT)
logger = logging.getLogger("comet.archiver")
logger.setLevel("INFO")


class Archiver:
    """Main class to run the comet archiver."""

    def __init__(self, data_dump_path, scrape_interval):

        startup_time = datetime.datetime.utcnow()
        config = {"data_dump_path": data_dump_path, "scrape_interval": scrape_interval}
        self.dir = data_dump_path
        self.interval = scrape_interval

        manager = Manager("localhost", DEFAULT_PORT)
        try:
            manager.register_start(startup_time, __version__)
            manager.register_config(config)
        except (CometError, ConnectionError) as exc:
            logger.error(
                "Comet archiver failed registering its startup and initial config: {}".format(
                    exc
                )
            )
            exit(1)

        self.loop = asyncio.get_event_loop()
        self.task = self.loop.create_task(self._scrape())

        # Open database connection
        chimedb.connect(read_write=True)

        # Create any missing table.
        chimedb.orm.create_tables("chimedb.dataset")

        # Buffer for entries to retry inserting later.
        # Some entries have references to others. If a referenced entry is not in the database yet,
        # the referencing will not be accepted and has to be buffered until it is accepted.
        self.entry_buffer = list()

    def run(self):
        """Run comet archiver."""
        logger.info(
            "Started CoMeT Archiver ({}), scraping path '{}' every {}.".format(
                __version__, self.dir, datetime.timedelta(seconds=self.interval)
            )
        )

        signal.signal(signal.SIGINT, lambda s, f: self.loop.stop())

        try:
            self.loop.run_forever()
        except asyncio.CancelledError:
            pass

    async def _scrape(self):
        try:
            time_last_scraped = datetime.datetime.min
            while True:

                # Find the dumped files
                dump_files = Path(self.dir).iterdir()
                dump_files = list(
                    filter(lambda x: x.as_posix().endswith("data.dump"), dump_files)
                )
                dump_times = [f.name[:-10] for f in dump_files]
                dump_times = [
                    datetime.datetime.strptime(t, TIMESTAMP_FORMAT) for t in dump_times
                ]
                if dump_files:
                    dump_times, dump_files = zip(*sorted(zip(dump_times, dump_files)))

                new_times = list(filter(lambda t: t > time_last_scraped, dump_times))

                for time in new_times:
                    dfile = dump_files[dump_times.index(time)]

                    # Only read files that don't have a lock file.
                    if not Path(self.dir).joinpath("{}.lock".format(dfile)).is_file():
                        # Set time_last_scraped to the timestamp of this file
                        time_last_scraped = time
                        with open(
                            Path(self.dir).joinpath(dfile).as_posix(), "r"
                        ) as json_file:
                            line_num = 0
                            for line in json_file:
                                line_num += 1
                                entry = json.loads(line)
                                self._insert_entry(entry, dfile, line_num)
                        logger.info(
                            "Archived {} entries from {}".format(line_num, dfile)
                        )

                # Check if there is anything in the buffer that is accepted now
                buffer_len = len(self.entry_buffer)
                if buffer_len:
                    logger.debug("Inserting {} entries from buffer.".format(buffer_len))
                for i in range(buffer_len):
                    entry = self.entry_buffer.pop(0)
                    self._insert_entry(entry, dfile)
                buffer_len = len(self.entry_buffer)
                if buffer_len:
                    logger.debug("Buffer still has {} entrie(s):".format(buffer_len))
                for e in self.entry_buffer:
                    logger.debug("{}".format(e))

                logger.debug(
                    "Scraping again in {}.".format(
                        datetime.timedelta(seconds=self.interval)
                    )
                )

                await asyncio.sleep(self.interval)
        except BaseException:
            self.loop.stop()
            raise

    def _insert_entry(self, entry, dfile, line_num="?"):
        if "state" in entry.keys():
            try:
                if not db.insert_state(entry):
                    # Retry later
                    self.entry_buffer.append(entry)
            except KeyError as key:
                logger.error(
                    "Entry in dump file {}:{} is missing key {}. "
                    "Skipping! This is the entry:\n{}".format(
                        Path(self.dir).joinpath(dfile), line_num, key, entry
                    )
                )
        elif "ds" in entry.keys():
            try:
                db.insert_dataset(entry)
            except KeyError as key:
                logger.error(
                    "Entry in dump file {}:{} is missing key {}. "
                    "Skipping! This is the entry:\n{}".format(
                        Path(self.dir).joinpath(dfile), line_num, key, entry
                    )
                )
        else:
            logger.warning(
                "Entry in dump file {}:{} is neither a state "
                "nor a dataset. Skipping! This is the entry:\n{}".format(
                    Path(self.dir).joinpath(dfile), line_num, entry
                )
            )

    def stop(self):
        """Stop the archiver."""
        self.task.cancel()
        chimedb.close()
