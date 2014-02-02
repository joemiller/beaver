import time
import uuid
import signal
import datetime

from systemd import journal


def system_timezone():
    """try to figure out the timezone of the current system. This is likely what
    timezone is used in the datetime objects returned by the journal.
    http://stackoverflow.com/questions/1111056/get-tz-information-of-the-system-in-python
    """
    return time.tzname[time.daylight]


def serializable_journal_entry(entry):
    """takes a journal entry returned by journal.Reader() and returns a dict that is
    serializable by json.dumps()
    """
    # Check for objects in the entry dictionary that may not be serializable.
    # Specifically, convert datetime and date objects to ISO 8601 format, and
    # convert UUID objects to hexadecimal strings.
    #
    # A special case is applied for '__MONOTONIC_TIMESTAMP', which contains a tuple of
    #    (datetime.timedelta(0, 1, 346747), UUID('d6d125ca-d752-4e81-9f2f-f92f60336bb0'))
    # with item 0 representing time since boot, and item 1 being the __BOOT_ID.
    # We drop the BOOT_ID and represent this only as a float of seconds since boot, eg:
    #   __MONOTONIC_TIMESTAMP = 1.346747
    #
    for key, value in entry.items():
        if key is '__MONOTONIC_TIMESTAMP':
            entry[key] = value[0].total_seconds()
        elif isinstance(value, datetime.timedelta):
            entry[key] = value.total_seconds()
        elif isinstance(value, datetime.date):
            entry[key] = value.isoformat() + 'Z'  # journald keeps UTC time
        elif isinstance(value, uuid.UUID):
            entry[key] = str(value)
    return entry


class Journal(object):

    def __init__(self, beaver_config, queue_consumer_function, callback, logger=None):
        self._filename = 'journal'  # callbacks and config need a filename. set a special identifier for the journal
        self._beaver_config = beaver_config
        self._callback = callback
        self._create_queue_consumer = queue_consumer_function
        self._logger = logger
        self._proc = None
        self._journal = None
        self._cursor_save_file = self._beaver_config.get_field('cursor_save_file', self._filename)
        self._cursor_save_interval = int(self._beaver_config.get_field('cursor_save_interval', self._filename))
        self._last_cursor_save_time = time.time()
        self._last_cursor_pos = None

        if not callable(self._callback):
            raise RuntimeError("Callback for journal is not callable")

        self.open_journal()

        # reset sighandlers in the child process to avoid timing issues in multiprocessing module during shutdown.
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.signal(signal.SIGQUIT, signal.SIG_DFL)
        signal.signal(signal.SIGINT, signal.SIG_DFL)

        self._read_cursor_from_file()
        if self._last_cursor_pos:
            self._logger.info('Seeking to last cursor pos {0} in journal.'.format(self._last_cursor_pos))
            self._seek_cursor(self._last_cursor_pos)
        else:
            self._logger.info('Seeking to end of journal')
            self._seek_tail()

    def cleanup(self, sig, frame):
        self.close()

    def close(self):
        self._save_cursor_to_file()
        if self._journal and not self._journal.closed:
            self._logger.debug('Journal.close(): closing journal')
            self._journal.close()
        if self._proc is not None and self._proc.is_alive():
            self._proc.terminate()
            self._proc.join()

    def __del__(self):
        self.close()

    def open_journal(self):
        self.close()
        self._journal = journal.Reader()

    # must call get_previous() after seek_tail(): https://bugs.freedesktop.org/show_bug.cgi?id=64614
    def _seek_tail(self):
        self._journal.seek_tail()
        self._journal.get_previous()

    def _seek_cursor(self, cursor):
        self._journal.seek_cursor(cursor)
        self._journal.get_previous()

    def _read_cursor_from_file(self):
        """read cursor position from _cursor_save_file. returns None if file does not exist or can't be read
        """
        self._last_cursor_pos = None
        if self._cursor_save_file:
            try:
                self._last_cursor_pos = open(self._cursor_save_file, 'r').read().strip('\n')
                self._logger.info('Read cursor {0} from save file {1}'.format(self._last_cursor_pos, self._cursor_save_file))
            except IOError as e:
                self._logger.info('Failed to read cursor from save file {0}: {1}'.format(self._cursor_save_file, e))

    def _save_cursor_to_file(self):
        if self._cursor_save_file and self._last_cursor_pos:
            self._logger.debug('Saving cursor {0} to file {1}'.format(self._last_cursor_pos, self._cursor_save_file))
            with open(self._cursor_save_file, 'w') as f:
                f.write(self._last_cursor_pos + '\n')
                self._last_cursor_save_time = time.time()

    def _read_journal_entries(self):
        """read one or more entries from the journal and execute the callback on each.
           This is called from self.loop() whenever a change is detected in the journal.
        """
        for e in self._journal:
            if e:
                self._callback_wrapper(e)
                if self._last_cursor_save_time + self._cursor_save_interval < time.time():
                    self._save_cursor_to_file()
            else:
                self._logger.debug('journal returned empty entry, done processing this batch.')
                break

    def _callback_wrapper(self, entry):
        """parse a journal entry and execute the callback on each
        """
        entry = serializable_journal_entry(entry)

        # extract required keys
        message = entry.pop('MESSAGE')
        ts = entry.pop('__REALTIME_TIMESTAMP')

        # remove __CURSOR from the message and return it so that we can save it later
        cursor = entry.pop('__CURSOR')

        # all remaining keys are added as additional @fields
        fields = entry

        args = {
            'fields': self._beaver_config.get_field('fields', self._filename),
            'filename': self._filename,
            'ignore_empty': self._beaver_config.get_field('ignore_empty', self._filename),
            'lines': [message],  # must be list
            'fields': fields,
            'timestamp': ts,
            'tags': self._beaver_config.get_field('tags', self._filename),
        }
        self._callback(('callback', args))
        self._last_cursor_pos = cursor

    def create_queue_consumer_if_required(self, interval=5.0):
        if not (self._proc and self._proc.is_alive()):
            self._proc = self._create_queue_consumer()

    def loop(self, async=False):
        """Start the loop.
        If async is True make one loop then return.
        """
        self.create_queue_consumer_if_required()

        # do an initial attempt to read any pending entries in the journal, then wait for more
        self._read_journal_entries()

        while True:
            r = self._journal.wait(-1)

            if r == journal.NOP:
                self._logger.debug('Journal.loop(): got journal.NOP')
            elif r == journal.APPEND:
                self._logger.debug('Journal.loop(): got journal.APPEND')
                self._read_journal_entries()
            elif r == journal.INVALIDATE:
                # @TODO: not sure what to do with these other than ignore them.
                # The journal library always seems to throw one as the first event after opening the journal.
                self._logger.debug('Journal.loop(): got journal.INVALIDATE')

            if async:
                return
