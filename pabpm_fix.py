import codecs
import csv
import hashlib
import os
import sys
import time
import zipfile
from datetime import datetime
from enum import Enum

user_header_en = 'First Name,Last Name,Birth year,Gender,Height/cm,Weight/kg,Email,Phone Number'
user_header_sr = 'Ime,Prezime,Godina rodjenja,Pol,Visina/cm,Tezina/kg,Email,Broj telefona'
user_header_fn = 'Ime,Prezime,Godina rodjenja,Pol,Visina/cm,Tezina/kg,Email,Broj telefona,' \
                 'Sistolni prag,Diastolni prag,ID,Doktor,Datum'
readings_header_en = 'Vreme,Datum,Sis,Dia,Sap,Pp,Puls,Polozaj,Mod'
readings_header_sr = 'Time,Date,Sys,Dia,Map,Pp,Hr,Position,Mode'
gender_map = {'Male': 'Muško', 'Female': 'Žensko'}
movement_map = {'Heavy moving': 'Ubrzano kretanje', 'Lying': 'Ležanje', 'Slight moving': 'Lagano kretanje',
                'Stand/Sit': 'Stajanje/sedenje'}
mode_map = {'Automatic': 'Automatsko', 'Manual': 'Manuelno'}


class FileData:

    def __init__(self, user, readings, appendices, min_date, max_date):
        self.user = user
        self.readings = readings
        self.appendices = appendices
        self.min_date = min_date
        self.max_date = max_date

    def set_user(self, user):
        user = user_row_fixed(user)
        self.user = user

    def add_reading(self, reading):
        reading = reading_row_fixed(reading)

        timestamp = time.mktime(datetime.strptime(reading[1] + ' ' + reading[0], '%d.%m.%Y %H:%M').timetuple())
        if timestamp < self.min_date:
            self.min_date = timestamp
        if timestamp > self.max_date:
            self.max_date = timestamp

        self.readings[timestamp] = reading

    def add_to_appendices(self, appendix):
        self.appendices.append(appendix)

    def is_mergeable_with_file(self, file):
        is_same_user = (self.user[0] == file.user[0]) and \
                       (self.user[1] == file.user[1]) and \
                       (self.user[2] == file.user[2]) and \
                       (self.user[4] == file.user[4]) and \
                       (self.user[5] == file.user[5])

        max_date_diff = 3 * 60 * 60  # 3 hours
        is_close_to = (file.min_date - self.max_date) < max_date_diff or (self.min_date - file.max_date) < max_date_diff

        return is_same_user and is_close_to

    def merge_file(self, file):
        for file_reading_key, file_reading_value in file.readings.items():
            self.add_reading(file_reading_value)

        if len(self.appendices) == 0:
            for file_appendix in file.appendices:
                self.add_to_appendices(file_appendix)

        if file.min_date < self.min_date:
            self.min_date = file.min_date
        if file.max_date > self.max_date:
            self.max_date = file.max_date

    def output_filename(self):
        min_date_string = datetime.utcfromtimestamp(self.min_date).strftime('%d.%m.%Y')
        max_date_string = datetime.utcfromtimestamp(self.max_date).strftime('%d.%m.%Y')

        return self.user[0] + '_' + self.user[1] + '_' + min_date_string + '_-_' + max_date_string + '.csv'

    def output_file(self):
        readings_lines = [','.join(readingValue) for readingKey, readingValue in sorted(self.readings.items())]
        all_readings_lines = '\n'.join(readings_lines)
        appendices_lines = [''.join(appendix) for appendix in self.appendices]
        all_appendices_lines = '\n'.join(appendices_lines)
        return user_header_fn + '\n' + ','.join(
            self.user) + '\n\n' + readings_header_en + '\n' + all_readings_lines + '\n' + all_appendices_lines


class ReadStage(Enum):
    pre_user = 1
    user = 2
    post_user = 3
    pre_readings = 4
    readings = 5
    appendix = 6


def detect_encoding(file_name):
    encodings = ['ascii', 'utf-16', 'utf-8']
    for encoding in encodings:
        try:
            fh = codecs.open(file_name, 'r', encoding=encoding)
            fh.readlines()
            fh.seek(0)
        except UnicodeDecodeError:
            pass
        else:
            return encoding


def hash_file(path, block_size=65536):
    a_file = open(path, 'rb')
    hash_maker = hashlib.md5()
    buf = a_file.read(block_size)
    while len(buf) > 0:
        hash_maker.update(buf)
        buf = a_file.read(block_size)
    a_file.close()

    return hash_maker.hexdigest()


def string_without_diacritics(input_string):
    return input_string.replace("š", "s").replace("Š", "S"). \
        replace("đ", "dj").replace("Đ", "Dj"). \
        replace("dž", "dž").replace("Dž", "Dz"). \
        replace("č", "c").replace("Č", "C"). \
        replace("ć", "c").replace("Ć", "C"). \
        replace("ž", "z").replace("Ž", "Z")


def comp_str(string):
    return string_without_diacritics(string).lower()


def line_matches(line, match_string):
    return comp_str(line) == comp_str(match_string)


def line_startswith(line, match_string):
    return comp_str(line).startswith(comp_str(match_string))


def user_row_fixed(user_row):
    local_row = user_row
    for key, value in gender_map.items():
        if comp_str(key) == comp_str(local_row[3]):
            local_row[3] = value

    return local_row


def reading_row_fixed(reading_row):
    local_row = reading_row
    for key, value in movement_map.items():
        if comp_str(key) == comp_str(local_row[7]):
            local_row[7] = value
    for key, value in mode_map.items():
        if comp_str(key) == comp_str(local_row[8]):
            local_row[8] = value

    return local_row


def process_file(csv_reader):
    current_file = FileData([], {}, [], 32536799999, 0)
    read_stage = ReadStage.pre_user

    file_rows = list(csv_reader)
    for row in file_rows:
        line = ','.join(row)
        if read_stage == ReadStage.pre_user:
            if line_startswith(line, user_header_en) or line_startswith(line, user_header_sr):
                read_stage = ReadStage.user
        elif read_stage == ReadStage.user:
            current_file.set_user(row)
            read_stage = ReadStage.post_user
        elif read_stage == ReadStage.post_user:
            read_stage = ReadStage.pre_readings
        elif read_stage == ReadStage.pre_readings:
            if line_startswith(line, readings_header_en) or line_startswith(line, readings_header_sr):
                read_stage = ReadStage.readings
        elif read_stage == ReadStage.readings:
            if len(row) > 0:
                current_file.add_reading(row)
            else:
                read_stage = ReadStage.appendix
                current_file.add_to_appendices(row)
        elif read_stage == ReadStage.appendix:
            current_file.add_to_appendices(row)

    return current_file


def append_to_files(file_to_append, files):
    file_merged = False
    for existing_file in files:
        if file_to_append.is_mergeable_with_file(existing_file):
            existing_file.merge_file(file_to_append)
            file_merged = True
            break

    if not file_merged:
        files.append(file_to_append)


def process_folder(parent_folder):
    backup_folder = os.path.join(parent_folder, 'backups')
    if not os.path.exists(backup_folder):
        os.makedirs(backup_folder)

    files = []

    now_string = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    my_zip = zipfile.ZipFile(os.path.join(backup_folder, 'backup_' + now_string + '.zip'), 'w', zipfile.ZIP_DEFLATED)

    hashes = []
    for dir_name, sub_dirs, file_list in os.walk(parent_folder):
        for filename in file_list:
            if filename.startswith('.') or filename.endswith('.csv') or filename.endswith('.zip'):
                continue

            path = os.path.join(dir_name, filename)

            my_zip.write(path, filename)

            file_hash = hash_file(path)
            if file_hash in hashes:
                os.remove(path)
                continue
            else:
                hashes.append(file_hash)

                with open(path, 'r', encoding=detect_encoding(path)) as csv_file:
                    csv_reader = csv.reader(csv_file, delimiter=',')
                    current_file = process_file(csv_reader)
                    append_to_files(current_file, files)

                os.remove(path)

    my_zip.close()

    return files


def export_files(files):
    for file in files:
        new_path = os.path.join(bp_folder, file.output_filename())
        new_file = open(new_path, 'w')
        new_file.write(file.output_file())
        new_file.close()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: python papbm_fix.py folder')
        sys.exit(1)
    else:
        bp_folder = sys.argv[1]
        if os.path.exists(bp_folder):
            files = process_folder(bp_folder)
            export_files(files)
        else:
            print('%s is not a valid path' % bp_folder)
            sys.exit(1)
