#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 22 11:22:38 2020

@author: eo
"""


# ---------------------------------------------------------------------------------------------------------------------
#%% Imports

import os
import subprocess

from shutil import which

import datetime as dt


# ---------------------------------------------------------------------------------------------------------------------
#%% Define classes

class Git_Caller:
    
    ''' Base class which provides minimal git usage functions '''
    
    # .................................................................................................................
    
    def __init__(self, git_folder_parent_path = None):
        
        # Try to find git folder, if not provided
        if git_folder_parent_path is None:
            this_file_path = os.path.abspath(os.path.realpath(__file__))
            this_folder_path = os.path.dirname(this_file_path)
            found_git_folder, git_folder_parent_path = \
            self.search_git_folder_backwards(this_folder_path, max_levels_to_search = 8,
                                             update_internal_pathing_on_success = False)
            
            # Warning if path not found
            if not found_git_folder:
                print("",
                      "Warning (Git Caller):"
                      "  Couldn't find git folder!",
                      sep = "\n")
        
        self.git_folder_parent_path = git_folder_parent_path        
        self._commit_format_arg = "--format='%h | %cd'"
    
    # .................................................................................................................
    
    @staticmethod
    def check_git_installed():
        
        ''' Helper function for checking whether git is installed on the system running this command '''
        
        # Check if we could actually call git on this system
        which_result = which("git")
        git_is_installed = (which_result is not None)
        
        return git_is_installed
    
    # .................................................................................................................
    
    def update_git_folder_parent_path(self, new_parent_folder_path):
        
        ''' Helper function used to update the parent path if needed '''
        
        self.git_folder_parent_path = new_parent_folder_path
    
    # .................................................................................................................
    
    def verify_git_folder(self):
        
        ''' Helper function used to check if we're pointing at a git folder '''
        
        # Bail in cases where parent path is not set
        if self.git_folder_parent_path is None:
            return False
        
        # Check if we're pointing at a git folder
        git_folder_path = os.path.join(self.git_folder_parent_path, ".git")
        have_git_folder = os.path.exists(git_folder_path)
        
        return have_git_folder
    
    # .................................................................................................................
    
    def get_version(self):
        
        ''' Simple function used to check git version '''
        
        # Initialize output
        version_str = None
        
        try:
            # Get version info from git
            # Example: ["git version 2.17.1"]
            git_version_str_as_list = self._run_git("--version")
            
            # Split string to get only the version itself
            git_version_str = git_version_str_as_list[0]
            split_version_strs_list = git_version_str.split()
            version_strs_list = [each_str for each_str in split_version_strs_list if "." in each_str]
            version_str = version_strs_list[0]
            
        except Exception:
            pass
        
        return version_str
    
    # .................................................................................................................
    
    def search_git_folder_backwards(self, starting_folder_path = None, max_levels_to_search = 5,
                                    update_internal_pathing_on_success = True):
        
        '''
        Function used to search for a .git folder, by going backwards from the provided folder path
        For example, given the initial folder path of:
            
            /folder1/folder2/folder3
        
        This function will search for a .git folder within folder3, then folder2, then folder1
        '''
        
        # Initialize outputs
        found_git_folder = False
        git_folder_parent_path = None
        
        # For clarity
        target_folder = ".git"
        
        # Use existing parent path if a starting path isn't provided
        if starting_folder_path is None:
            starting_folder_path = self.git_folder_parent_path
        
        # Search 'backwards' from starting path to try to find the git folder
        parent_path = starting_folder_path
        for k in range(max_levels_to_search):
            
            # Get a listing of all folders in the current parent folder path
            file_names_list = os.listdir(parent_path)
            file_paths_list = [os.path.join(parent_path, each_name) for each_name in file_names_list]
            folder_paths_list = [each_path for each_path in file_paths_list if os.path.isdir(each_path)]
            
            # Stop searching if we find a git folder
            target_path = os.path.join(parent_path, target_folder)
            found_git_folder = (target_path in folder_paths_list)
            if found_git_folder:
                git_folder_parent_path = parent_path
                break
            
            # Check if we can go 'backwards' one more folder
            new_parent_path = os.path.dirname(parent_path)
            cant_continue = (new_parent_path == parent_path)
            if cant_continue:
                break
            
            # If we get here, we can continue searching
            parent_path = new_parent_path
        
        # Record the git folder path if we found it
        if found_git_folder and update_internal_pathing_on_success:
            self.git_folder_parent_path = git_folder_parent_path
        
        return found_git_folder, git_folder_parent_path
    
    # .................................................................................................................
    
    def search_git_folder_forwards(self, starting_folder_path = None,
                                   update_internal_pathing_on_success = True):
        
        '''
        Function used to search for a git folder, by going forwards from the provided git folder path
        For example, given the initial folder path of:
            
            /folder1/folder2/folder3
        
        This function will search all folders inside of folder3, and then all folders inside of those folders etc.
        '''
        
        # Initialize outputs
        found_git_folder = False
        git_folder_parent_path = None
        
        # For clarity
        target_folder = ".git"
        
         # Use existing parent path if a starting path isn't provided
        if starting_folder_path is None:
            starting_folder_path = self.git_folder_parent_path
        
        # Search 'forwards' from starting path to try to find the git folder
        for each_parent_path, each_subfolder_list, _ in os.walk(starting_folder_path):
            
            # Stop searching if we find a git folder within the current parent folder
            found_git_folder = (target_folder in each_subfolder_list)
            if found_git_folder:
                git_folder_parent_path = each_parent_path
                break
        
        # Record the git folder path if we found it
        if found_git_folder and update_internal_pathing_on_success:
            self.git_folder_parent_path = git_folder_parent_path
        
        return found_git_folder, git_folder_parent_path
    
    # .................................................................................................................
    
    def _run_git(self, git_command, *command_strs, suppress_errors = True):
        
        # Build list of arguments to use with subprocess call
        cmd_list = ["git", "-C", self.git_folder_parent_path, git_command, *command_strs]
        
        # Run git with captured output & look for errors
        subproc_result = subprocess.run(cmd_list, stdout = subprocess.PIPE, stderr = subprocess.DEVNULL)
        if subproc_result.returncode != 0:
            
            # We may have failed because git isn't even installed...
            git_installed = self.check_git_installed()
            if not git_installed:
                raise RuntimeError("Error calling git... Not installed on system!")
            
            # We may have failed because the folder we're pointing doesn't have a git init
            has_git_folder = self.verify_git_folder()
            if not has_git_folder:
                raise TypeError("Error calling git... Not using a git-enabled folder!")
            
            # If we get here, the git call probably failed for some other reason
            print(subproc_result.returncode)
            raise AttributeError("Error calling git! Used command:\n{}".format(" ".join(cmd_list)))
        
        # Grab returned byte-str and split it into separate strings (by newline) in a list
        returned_byte_str = subproc_result.stdout
        returned_str = returned_byte_str.decode("utf-8")
        output_str_list = [each_str.strip("'") for each_str in returned_str.splitlines()]
        
        return output_str_list
    
    # .................................................................................................................
    
    def _parse_git_datetimes(self, datetime_str):
        
        '''
        Helper function which converts git commit datetime strings into python datetime objects
        Example git datetime str:
            "Wed Aug 5 14:09:05 2020 -0400"
        '''
        
        # For clarity
        git_datetime_format_str = "%a %b %d %H:%M:%S %Y %z"
        
        return dt.datetime.strptime(datetime_str, git_datetime_format_str)
    
    # .................................................................................................................
    # .................................................................................................................


class Git_Reader(Git_Caller):
    
    ''' Class used to perform read-only functions, using git '''
    
    # .................................................................................................................
    
    def __init__(self, git_folder_parent_path = None):
        
        # Inherit from parent class        
        super().__init__(git_folder_parent_path)
    
    # .................................................................................................................
    
    def log(self, *command_strs):
        return self._run_git("log", *command_strs)
    
    # .................................................................................................................
    
    def diff(self, *command_strs):
        return self._run_git("diff", *command_strs)
    
    # .................................................................................................................
    
    def get_full_commit_id(self, short_commit_id_str):
        
        try:
            message_str_list = self.log("-n", "1", "--format=%H", short_commit_id_str)
        except AttributeError:
            message_str_list = ["??? ({})".format(short_commit_id_str)]
        
        # Only grab the first string (there may be empty lines after), which should contain the full id
        full_id_str = message_str_list[0]
        
        return full_id_str
    
    # .................................................................................................................
    
    def get_tags_for_commit(self, commit_id_str):
        
        tags_list = []
        try:
            tags_list = self._run_git("tag", "--points-at", commit_id_str)
        except AttributeError:
            pass
        
        return tags_list
    
    # .................................................................................................................
    
    def get_commit_message(self, commit_id_str, include_header_info = True):
        
        ''' Helper function used to obtain commit messages '''
        
        # Get commit message if possible
        try:
            message_str_list = self.log("-n", "1", "--format=%B", commit_id_str)
        except AttributeError:
            message_str_list = ["Error! Can't find commit message for {}...".format(commit_id_str)]
            include_header_info = False
        
        # We're done now if we don't need the full id
        if not include_header_info:
            return message_str_list
        
        # Get full id and add as a 'header' entry to message list
        full_commit_id = self.get_full_commit_id(commit_id_str)
        id_header = "Commit: {}".format(full_commit_id)
        
        # Get the commit tags to add under the commit ID header
        tags_list = self.get_tags_for_commit(commit_id_str)
        tags_str = ", ".join(tags_list)
        tags_header = "Tags: {}".format(tags_str)
        
        # Combine  full ID + tag listing to commit message data before sending out
        augmented_message_str_list = [id_header, tags_header, ""] + message_str_list
        
        return augmented_message_str_list
    
    # .................................................................................................................
    
    def get_newer_commits(self, fetch_first = False, max_number_of_commits = 5):
        
        # If needed, get latest info available
        if fetch_first:
            self.fetch()
        
        # Use 'git log' to get raw commit listing
        log_results_list = self.log("HEAD..origin/master", self._commit_format_arg)
        num_to_use = min(len(log_results_list), max_number_of_commits)
        
        # Now break listing into commit IDs & relative dates
        commit_ids_list = []
        commit_tags_list = []
        commit_dates_list = []
        for each_entry in log_results_list[:num_to_use]:
            each_id, each_date_str = each_entry.split(" | ")
            each_tags = self.get_tags_for_commit(each_id)
            each_dt = self._parse_git_datetimes(each_date_str)
            commit_ids_list.append(each_id)
            commit_tags_list.append(each_tags)
            commit_dates_list.append(each_dt)
        
        return commit_ids_list, commit_tags_list, commit_dates_list
    
    # .................................................................................................................
    
    def get_current_commit(self):
        
        # First use 'git log' to get raw commit listing
        log_results_list = self.log("-1", self._commit_format_arg)
        current_entry = log_results_list[0]
        
        # Now break listing into commit IDs & relative dates
        commit_id, commit_date_str = current_entry.split(" | ")
        commit_tags_list = self.get_tags_for_commit(commit_id)
        commit_dt = self._parse_git_datetimes(commit_date_str)
        
        return commit_id, commit_tags_list, commit_dt
    
    # .................................................................................................................
    
    def get_older_commits(self, max_number_of_commits = 3):
        
        # Handle special case of negative/zero max commits
        if max_number_of_commits < 1:
            return [], []
        
        # Create the log argument for getting older commits
        safe_number_of_commits = 1 + int(max_number_of_commits)
        num_entries_arg = "-n {}".format(safe_number_of_commits)
        
        # Use 'git log' to get raw commit listing
        log_results_list = self.log(num_entries_arg, self._commit_format_arg)
        older_entries_list = log_results_list[1:]
        
        # Now break listing into commit IDs & relative dates
        commit_ids_list = []
        commit_tags_list = []
        commit_dates_list = []
        for each_entry in older_entries_list:            
            each_id, each_date_str = each_entry.split(" | ")
            each_tags = self.get_tags_for_commit(each_id)
            each_dt = self._parse_git_datetimes(each_date_str)
            commit_ids_list.append(each_id)
            commit_tags_list.append(each_tags)
            commit_dates_list.append(each_dt)
        
        return commit_ids_list, commit_tags_list, commit_dates_list
    
    # .................................................................................................................
    
    def get_commit_listings(self, max_listings = 6):
        
        # Get the current entry
        current_commit_id, current_commit_tags_list, current_commit_dt = self.get_current_commit()
        
        # Grab all newer commits
        max_new = (max_listings - 1)
        newer_ids_list, newer_tags_list, newer_dts_list = self.get_newer_commits(max_number_of_commits = max_new)
        num_new = len(newer_ids_list)
        
        # Determine how many 'old' entries we should get, in case we have too many newer entries
        num_old_to_get = (max_listings - num_new - 1)
        older_ids_list, older_tags_list, older_dts_list = self.get_older_commits(max_number_of_commits = num_old_to_get)
        
        # Build list of dictionaries containing commit ID, date and 'in-use' status for future commits
        newer_listings = []
        for each_new_id, each_tags_list, each_new_dt in zip(newer_ids_list, newer_tags_list, newer_dts_list):
            one_listing = create_new_commit_listing(each_new_id, each_tags_list, each_new_dt)
            newer_listings.append(one_listing)
        
        # Create listing entry for the current commit
        current_listing = \
        create_new_commit_listing(current_commit_id, current_commit_tags_list, current_commit_dt, in_use = True)
        
        # Create listing for older commits
        older_listings = []
        for each_old_id, each_tags_list, each_old_dt in zip(older_ids_list, older_tags_list, older_dts_list):
            one_listing = create_new_commit_listing(each_old_id, each_tags_list, each_old_dt)
            older_listings.append(one_listing)
        
        # Finally, combine newer/current and older commits into a single listing, with limits on list size
        output_commit_listings = newer_listings + [current_listing] + older_listings
        
        return output_commit_listings
    
    # .................................................................................................................
    # .................................................................................................................

class Git_Writer(Git_Caller):
    
    ''' Class used to perform write operations using git '''
    
    # .................................................................................................................
    
    def __init__(self, git_folder_parent_path = None):
        
        # Inherit from parent class        
        super().__init__(git_folder_parent_path)
    
    # .................................................................................................................
    
    def checkout(self, branch_or_commit_id, *command_strs):
        return self._run_git("checkout", branch_or_commit_id, *command_strs)
    
    # .................................................................................................................
    
    def fetch(self, *command_strs, remote_name = "origin", branch_name = "master"):
        return self._run_git("fetch", remote_name, branch_name, *command_strs)
    
    # .................................................................................................................
    
    def set_commit(self, commit_id_str):
        
        # Initialize output
        set_success = False
        previous_commit_id, _, _ = self.get_current_commit()
        
        try:
            # Run checkout and make sure we mark our checkout as a success if nothing goes wrong
            self.checkout(commit_id_str)
            set_success = True
            
        except AttributeError:
            # We end up here if the git command fails
            set_success = False
            
        # Merge commits if we jump ahead
        try:
            commits_ahead_of_master_list = self.log("master..HEAD", "--format=%h")
            need_to_merge = (len(commits_ahead_of_master_list) > 0)
            if need_to_merge:
                self.checkout("master")
                self._run_git("merge", commit_id_str)
            
        except AttributeError:
            # Git run command failed for some reason, we'll just give up on merging for now
            pass
        
        # Try to fix detach head state if needed
        try:
            master_commit_id = self._run_git("rev-parse", "master")
            head_commit_id = self._run_git("rev-parse", "HEAD")
            can_reattach = (master_commit_id == head_commit_id)
            if can_reattach:
                self.checkout("master")
            
        except AttributeError:
            # Git run command failed for some reason, we'll just give up on cleaning the detached head state
            pass
        
        # Try to get the current id, which may have changed if we succeeded
        current_commit_id, _, _ = self.get_current_commit()
        
        return set_success, previous_commit_id, current_commit_id
    
    # .................................................................................................................
    # .................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Define Functions

# .....................................................................................................................

def create_new_commit_listing(commit_id, commit_tags_list, commit_dt,
                              *,
                              in_use = False,
                              datetime_string_format = "%Y/%m/%d %I:%M:%S %p (%z UTC)",
                              tag_separator = ", "):
    
    ''' Helper function for creating consistently formatted commit listing for easier manipulation '''
    
    # Convert datetime to a string for display
    commit_date_str = commit_dt.strftime(datetime_string_format)
    
    # Create string to represent tags
    has_tags = (len(commit_tags_list) > 0)
    commit_tag_str = commit_tags_list[0] if has_tags else ""
    
    return {"commit_id": commit_id,
            "commit_tag": commit_tag_str,
            "commit_date": commit_date_str,
            "in_use": in_use}

# .....................................................................................................................
# .....................................................................................................................


# ---------------------------------------------------------------------------------------------------------------------
#%% Demo

if __name__ == "__main__":
    
    ex_git = Git_Caller()
    print("GIT PATH:", ex_git.git_folder_parent_path)
    if ex_git.verify_git_folder():
        print(ex_git.get_version())
    


# ---------------------------------------------------------------------------------------------------------------------
#%% Scrap


