# This file contains the Library class, which is the main interface for the user to interact with the library.

from .subject import Subject
import os
import shutil
from . import utils
import duckdb

class Library:
    def __repr__(self):
        return f"Quiver.library {self.library}"

    def __init__(self, library):
        library_path = utils.get_path()
        if not utils.path_exists(library_path):
            os.mkdir(library_path)
        self.library = utils.make_path(library_path, library)
        if not utils.path_exists(self.library):
            os.mkdir(self.library)
        self.metadata = utils.read_metadata(self.library)
        self.subjects = self.list_subjects()
        self.db = duckdb.connect()

    def save_library_metadata(self,metadata):
        """
        Save metadata to the library, should have
        metadata['description'] = "some description of the data in the library'
        :param metadata:
        :return:
        """
        if utils.path_exists(utils.make_path(self.library, 'quiver_metadata.json')):
            existing_metadata = utils.read_metadata(utils.make_path(self.library))
            for e in existing_metadata:
                if e not in metadata:
                    metadata[e] = existing_metadata[e]
        utils.write_metadata(self.library, metadata)
        self.metadata = metadata
        return True

    def _create_subject(self, subject, metadata=None, overwrite=False):
        # create subject (subdir)
        subject_path = utils.make_path(self.library, subject)
        if utils.path_exists(subject_path):
            if overwrite:
                self.delete_subject(subject)
            else:
                raise ValueError(
                    "Subject exists! To overwrite, use `overwrite=True`")

        os.makedirs(subject_path)
        os.makedirs(utils.make_path(subject_path, "_snapshots"))

        # update subjects
        self.subjects = self.list_subjects()

        # return the subject
        return Subject(subject, self.library)

    def delete_subject(self, subject, confirm=True):
        # delete subject (subdir)
        if subject not in self.subjects:
            raise ValueError(f"Subject {subject} does not exist")
        if confirm:
            confirm = input(f"Delete subject {subject}? (y/n)")
            if confirm.lower() != "y":
                print("Deletion aborted")
                return False
        shutil.rmtree(utils.make_path(self.library, subject))
        # update subjects
        self.subjects = self.list_subjects()
        return True

    def list_subjects(self):
        # lists subjects (subdirs)
        return utils.subdirs(self.library)

    def subject(self, subject, metadata=None, overwrite=False):
        if subject in self.subjects and not overwrite:
            return Subject(subject, self.library)

        # create it
        if subject not in self.subjects:
            confirm = input(f"Subject {subject} does not exist. Create it? (y/n)")
            if confirm.lower() != "y":
                print("Aborted")
                return None
            else:
                self._create_subject(subject, overwrite)

        return Subject(subject, self.library)

    def item(self, subject, item):
        # bypasses subject
        return self.subject(subject).item(item)

    def stop_db(self):
        self.db.close()
        return True

    def start_db(self):
        self.db = duckdb.connect()
        return self.db


