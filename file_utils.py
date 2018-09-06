from __future__ import print_function
from datetime import timedelta
import re


_MSG_HDF5_FILENAME_REGEX_PATTERN = "MSG(?P<msg_id>[0-9])-SEVI-MSG[0-9]{1,2}-[0-9]{4}-[A-Z]{2}-(?P<date_year>[0-9]{4})(?P<date_month>[0-9]{2})(?P<date_day>[0-9]{2})(?P<date_hour>[0-9]{2})(?P<date_min>[0-9]{2})[0-9]{2}.*h5(?P<compression>.[\w]*)?"
#"MSG(?P<msg_id>[0-9])-SEVI-MSG[0-9]{1,2}-[0-9]{4}-[A-Z]{2}-@@@(?P<date_min>[0-9]{2})[0-9]{2}.*h5(?P<compression>.[\w]*)?"
_MSG_HDF5_FILENAME_REGEX = re.compile(_MSG_HDF5_FILENAME_REGEX_PATTERN)
_DEFAULT_FILE_PREFIX = "%Y/%m/%d/%Y%m%d_%H%M/"

def unbz2_file(filename):
    """Returns a file like object where the data is stored"""

    import tempfile
    import bz2
    if filename.endswith('bz2'):
        bz2_file = bz2.BZ2File(filename)
        tmp_file = tempfile.NamedTemporaryFile(prefix="msg_", suffix=".h5")
        print("created temp file for packed data: ", str(filename), "->", str(tmp_file.name))
        try:
            tmp_file.write(bz2_file.read())
            tmp_file.flush()
        except IOError:
            import traceback
            traceback.print_exc()
            print("Failed to unpack (bz2) file:", str(file_name))
            tmp_file.close()
            return None

        return tmp_file
    return None

def find_date_filename(date_time, base_path, filename_regex_pattern = _MSG_HDF5_FILENAME_REGEX_PATTERN, prefixes = [], minutes_range = 5):
    """ Finds a filename with a matching date pattern"""

    from datetime import datetime
    import os

    date_time_lower_bound = date_time - timedelta(minutes=minutes_range)
    date_time_upper_bound = date_time + timedelta(minutes=minutes_range)
    date_time_filename_regex = _MSG_HDF5_FILENAME_REGEX # re.compile(filename_regex_pattern.replace("@@@", datetime.strftime(date_time, "%Y%m%d%H")))

    path_with_prefix = None
    for pre in prefixes:
        new_path_with_prefix = base_path + "/" + datetime.strftime(date_time, pre)
        if os.path.isdir(new_path_with_prefix):
            path_with_prefix = new_path_with_prefix
            break

    if path_with_prefix is not None:

        for filename in os.listdir(path_with_prefix):
            # print(filename)
            match = date_time_filename_regex.match(filename)
            if match:
                file_date_time = datetime(
                    int(match.group('date_year')),
                    int(match.group('date_month')),
                    int(match.group('date_day')),
                    int(match.group('date_hour')),
                    int(match.group('date_min')),
                )

                if date_time_lower_bound <= file_date_time <= date_time_upper_bound:
                    print(date_time_lower_bound, "<=", file_date_time, "<=", date_time_upper_bound)
                    return filename, match.groupdict()

    return None, None
