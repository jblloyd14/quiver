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
            self._create_library(library)
        self.metadata = utils.read_metadata(self.library)
        self.subjects = self.list_subjects()
        self.db = duckdb.connect()

    def _create_library(self, library_name):
        """
        Create a new library after user confirmation.
        
        Args:
            library_name (str): Name of the library to create
            
        Returns:
            None
        """
        confirm = input(f"You are creating a new library '{library_name}'. Do you want to proceed? (y/n) ")
        if confirm.lower() != 'y':
            print("Library creation aborted")
            return
        os.mkdir(self.library)
        print(f"Library '{library_name}' created successfully")

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

        subject_path.mkdir(parents=True, exist_ok=True)
        utils.make_path(subject_path, "_snapshots").mkdir(parents=True, exist_ok=True)

        # update subjects
        self.subjects = self.list_subjects()
        if metadata is not None:
            if utils.path_exists(utils.make_path(self.library, subject, 'quiver_metadata.json')):
                existing_metadata = utils.read_metadata(utils.make_path(self.library, subject))
                for e in existing_metadata:
                    if e not in metadata:
                        metadata[e] = existing_metadata[e]
            utils.write_metadata(utils.make_path(self.library, subject), metadata)
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

    def subject(self, subject, partition_on=None, metadata=None, overwrite=False):
        """
        Get a subject object, creating it if it does not exist.

        :param subject: str; name of the subject to retrieve or create
        :param partition_on: str or list of str; partition columns for the subject, defaults to None
        :param metadata: dict, optional; metadata for the subject, should contain 'description' key
        :param overwrite: bool; if True, overwrite the subject if it exists
        :return: Subject object or None if creation/overwrite is aborted
        """
        if subject in self.subjects and not overwrite:
            return Subject(subject, self.library)

        # Prepare metadata with partition info
        metadata = metadata or {}
        if 'partition_on' not in metadata and partition_on is not None:
            metadata['partition_on'] = [partition_on] if isinstance(partition_on, str) else partition_on

        # Get confirmation for create/overwrite
        action = "overwrite" if subject in self.subjects else "create"
        confirm = input(f"Subject {subject} does not exist. Create it? (y/n)" if action == "create"
                        else f"You are about to overwrite Subject {subject}. Confirm? (y/n)")

        if confirm.lower() != "y":
            print("Aborted")
            return None

        self._create_subject(subject, metadata=metadata, overwrite=overwrite)
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
