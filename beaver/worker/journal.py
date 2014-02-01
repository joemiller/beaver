import time
import uuid
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
        if isinstance(value, datetime.date):
            entry[key] = value.isoformat() + 'Z'  # journald keeps UTC time
        elif isinstance(value, uuid.UUID):
            entry[key] = str(value)
    return entry


class Journal(object):

    def __init__(self, beaver_config, queue_consumer_function, callback, logger=None):
        self._beaver_config = beaver_config
        self._callback = callback
        self._create_queue_consumer = queue_consumer_function
        self._logger = logger
        self._proc = None
        self._journal = None

        # TODO: set state_file_path from beaver_config

        if not callable(self._callback):
            raise RuntimeError("Callback for journal is not callable")

        self.open_journal()

    def close(self):
        if self._journal and not self._journal.closed:
            self._logger.debug('Journal.close(): closing journal')
            self._journal.close()

    def __del__(self):
        self.close()

    def open_journal(self):
        self.close()
        self._journal = journal.Reader()
        self._seek_tail()

    # must call get_previous() after seek_tail(): https://bugs.freedesktop.org/show_bug.cgi?id=64614
    def _seek_tail(self):
        self._journal.seek_tail()
        self._journal.get_previous()

    def _get_last_cursor(self):
        # @TODO
        pass

    def _read_last_cursor(self):
        # @TODO
        pass

    def _save_last_cursor(self):
        # @TODO
        pass

    def _read_journal_entries(self):
        for e in self._journal:
            if e:
                self._callback_wrapper(e)
            else:
                self._logger.debug('journal returned empty entry, done processing this batch.')
                return

    def _callback_wrapper(self, entry):
        entry = serializable_journal_entry(entry)

        # special "filename" to identify journald settings in the beaver config
        filename = '/systemd-journald'

        # extract required keys
        message = entry.pop('MESSAGE')
        ts = entry.pop('__REALTIME_TIMESTAMP')

        # remove fields that we don't want to send to logstash
        entry.pop('__CURSOR')

        # all remaining keys are added as additional @fields
        fields = entry

        args = {
            'fields': self._beaver_config.get_field('fields', filename),
            'filename': filename,
            #'format': self._beaver_config.get_field('format', filename),  # @TODO: shouldn't be necessary
            'ignore_empty': self._beaver_config.get_field('ignore_empty', filename),
            'lines': [message],  # must be list
            'fields': fields,
            'timestamp': ts,
            'tags': self._beaver_config.get_field('tags', filename),
            #'type': self._beaver_config.get_field('type', filename),  # @TODO: not sure what to do. doesn't make sense for journald
        }
        self._logger.debug('callback args: %s' % args)
        self._callback(('callback', args))
        #self._callback(('callback', {
        #}))


    def loop(self, async=False):
        """Start the loop.
        If async is True make one loop then return.
        """
        while True:
            if not (self._proc and self._proc.is_alive()):
                self._proc = self._create_queue_consumer()

            r = self._journal.wait(1000)

            if r == journal.NOP:
                self._logger.debug('Journal.loop(): got journal.NOP')
            elif r == journal.APPEND:
                self._logger.debug('Journal.loop(): got journal.APPEND')
                self._read_journal_entries()
                # @TODO: save cursor
            elif r == journal.INVALIDATE:
                # @TODO: not sure what to do with these. The library always seems to
                # throw one as the first event after opening the journal.
                self._logger.debug('Journal.loop(): got journal.INVALIDATE')

            if async:
                return
